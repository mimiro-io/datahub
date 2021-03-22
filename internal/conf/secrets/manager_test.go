package secrets

import (
	"errors"
	"github.com/franela/goblin"
	"go.uber.org/zap"
	"testing"
)

func TestNewManager(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("Testing Secrets Creation", func() {
		g.It("should return noop manager when given noop", func() {
			mngr, err := NewManager("noop", "test", zap.NewNop().Sugar())
			if err != nil {
				g.Fail(err)
			}
			switch mngr.(type) {
			case *NoopStore:
				// ok
			default:
				g.Fail(errors.New("store is not NoopStore"))
			}
		})

	})
}
