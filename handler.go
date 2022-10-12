package gitremote

import (
	"context"

	"github.com/go-git/go-git/v5"

	"github.com/drgomesp/git-remote-ipldprime/core"
)

type ProtocolHandler interface {
	Initialize(tracker *core.Tracker, repo *git.Repository) error
	Capabilities() string
	List(forPush bool) ([]string, error)
	Push(ctx context.Context, localRef string, remoteRef string) (string, error)
	ProvideBlock(identifier string, tracker *core.Tracker) ([]byte, error)
	Finish() error
}
