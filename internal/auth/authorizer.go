package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func New(model, poilcy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, poilcy)

	return &Authorizer{
		enforcer: enforcer,
	}
}

// Implement Authorize interface
func (a *Authorizer) Authorize(subject, object, action string) error {
	if !a.enforcer.Enforce(subject, object, action) {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject,
			action,
			object,
		)

		st := status.New(codes.PermissionDenied, msg)

		return st.Err()
	}

	return nil
}
