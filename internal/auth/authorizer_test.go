package auth

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wuxl-lang/proglog/config"
)

func TestAuthorizer(t *testing.T) {
	authorizer := New(config.ACLModelFile, config.ACLPolicyFile)

	err := authorizer.Authorize("nobody", "*", "produce")
	require.Error(t, err)
}
