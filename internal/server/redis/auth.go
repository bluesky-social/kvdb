package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/bluesky-social/kvdb/internal/types"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	errInvalidCredentials = errors.New("invalid credentials")
)

func (s *session) handlePing(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handlePing") // nolint
	defer span.End()

	if len(args) > 1 {
		return "", recordErr(span, fmt.Errorf("invalid number of arguments"))
	}

	res := "+PONG\r\n"
	if len(args) > 0 {
		// echo back the first argument
		arg, err := extractStringArg(args[0])
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse argument: %w", err))
		}
		res = resp.FormatSimpleString(arg)
	}

	span.SetStatus(codes.Ok, "ping handled")
	return res, nil
}

func (s *session) handleAuth(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleAuth") // nolint
	defer span.End()

	s.userMu.RLock()
	alreadyAuthed := s.user != nil
	s.userMu.RUnlock()
	if alreadyAuthed {
		span.SetStatus(codes.Ok, "already authed")
		return resp.FormatError(fmt.Errorf("session is already authenticated")), nil
	}

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	username, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse username argument: %w", err))
	}

	pass, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse password argument: %w", err))
	}

	user, err := s.getUser(username)
	if err != nil {
		return "", recordErr(span, err)
	}
	if user == nil {
		span.SetStatus(codes.Ok, "user not found")
		return resp.FormatError(errInvalidCredentials), nil
	}
	if !user.Enabled {
		span.SetStatus(codes.Ok, "user is disabled")
		return resp.FormatError(errInvalidCredentials), nil
	}
	if err := comparePasswords(user.PasswordHash, pass); err != nil {
		span.SetStatus(codes.Ok, "invalid password")
		return resp.FormatError(errInvalidCredentials), nil
	}

	// set the user directory on the session
	userDir, err := s.dirs.redis.CreateOrOpen(s.fdb, []string{user.Username}, nil)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to initialize user directory: %w", err))
	}

	objDir, err := userDir.CreateOrOpen(s.fdb, []string{"obj"}, nil)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to initialize user object directory: %w", err))
	}

	metaDir, err := userDir.CreateOrOpen(s.fdb, []string{"meta"}, nil)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to initialize user meta directory: %w", err))
	}

	uidDir, err := userDir.CreateOrOpen(s.fdb, []string{"uid"}, nil)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to initialize user uid directory: %w", err))
	}

	reverseUIDDir, err := userDir.CreateOrOpen(s.fdb, []string{"ruid"}, nil)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to initialize user reverse uid directory: %w", err))
	}

	s.userMu.Lock()
	s.user = &sessionUser{
		objDir:        objDir,
		metaDir:       metaDir,
		uidDir:        uidDir,
		reverseUIDDir: reverseUIDDir,
		user:          user,
	}
	s.userMu.Unlock()

	span.SetStatus(codes.Ok, "auth handled")
	return resp.FormatSimpleString("OK"), nil
}

// Implements a small subset of the standard redis functionality for creating a new user.
//
// Example: `ACL SETUSER newusername on >password123 ~* &* +@all`
func (s *session) handleACL(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleACL") // nolint
	defer span.End()

	//
	// Only admin users are allowed to create other users
	//

	err := func() error {
		s.userMu.RLock()
		defer s.userMu.RUnlock()

		if !userIsAdmin(s.user.user) {
			return fmt.Errorf("only admin users are allowed to run ACL commands")
		}

		return nil
	}()
	if err != nil {
		return "", recordErr(span, err)
	}

	//
	// Validate and parse basic arguments required to create a user and
	// password that's allowed read+write on all keys and commands
	//

	if err := validateNumArgs(args, 4); err != nil {
		return "", recordErr(span, err)
	}

	cmd, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse acl command argument: %w", err))
	}
	if strings.ToLower(cmd) != "setuser" {
		return "", recordErr(span, fmt.Errorf("only SETUSER is supported"))
	}

	username, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse username argument: %w", err))
	}

	on, err := extractStringArg(args[2])
	if err != nil || strings.ToLower(on) != "on" {
		return "", recordErr(span, fmt.Errorf(`failed to parse "on" argument`))
	}

	password, err := extractStringArg(args[3])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse password argument: %w", err))
	}
	if !strings.HasPrefix(password, ">") {
		return "", recordErr(span, fmt.Errorf(`only ">" passwords are supported`))
	}
	password = strings.TrimPrefix(password, ">")

	if err := validateUsername(username); err != nil {
		return "", recordErr(span, fmt.Errorf("invalid username: %w", err))
	}

	if err := validatePassword(password); err != nil {
		return "", recordErr(span, fmt.Errorf("invalid password: %w", err))
	}

	//
	// Ensure the user does not already exist
	//

	existing, err := s.getUser(username)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to check if user already exists: %w", err))
	}
	if existing != nil {
		return "", recordErr(span, fmt.Errorf("failed to create user because a user with username %q already exists", username))
	}

	//
	// Create the user in the users directory
	//

	passHash, err := hashPassword(password)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to hash password: %w", err))
	}

	now := timestamppb.Now()
	user := &types.User{
		Username:     username,
		PasswordHash: passHash,
		Created:      now,
		LastLogin:    now,
		Enabled:      true,
		Rules: []*types.UserACLRule{{
			Level: types.UserAccessLevel_USER_ACCESS_LEVEL_READ_WRITE,
		}},
	}

	if err := s.setProtoItem(s.userKey(username), user); err != nil {
		return "", recordErr(span, fmt.Errorf("failed to save user to database: %w", err))
	}

	span.SetStatus(codes.Ok, "acl handled")
	return resp.FormatSimpleString("OK"), nil
}

func validateUsername(u string) error {
	if u == "" {
		return fmt.Errorf("username cannot be empty")
	}
	if strings.HasPrefix(u, "_") {
		return fmt.Errorf("username starts with a restricted character")
	}
	if containsWhitespace(u) {
		return fmt.Errorf("username cannot contain whitespace")
	}

	return nil
}

func validatePassword(p string) error {
	if p == "" {
		return fmt.Errorf("password cannot be empty")
	}
	if len(p) < 8 {
		return fmt.Errorf("password is too short")
	}
	if containsWhitespace(p) {
		return fmt.Errorf("password cannot contain whitespace")
	}

	return nil
}
