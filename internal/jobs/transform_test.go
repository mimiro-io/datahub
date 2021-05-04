package jobs

import (
	"encoding/base64"
	"github.com/DataDog/datadog-go/statsd"
	"strings"
	"testing"

	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/zap"
)

func TestTransform(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("A Javascript transformation", func() {
		g.It("Should return a go error when the script fails", func() {
			// a failing function
			js := ` // fail.js
					function transform_entities(entities) {
						for (e of entities) {
							var bodyEvent = GetProperty(e, prefix, "failField");
							Log(bodyEvent.substring(0, 3));
					  	}
						return entities;
					}`
			f := base64.StdEncoding.EncodeToString([]byte(js))

			transform, err := newJavascriptTransform(zap.NewNop().Sugar(), f, nil)
			g.Assert(err).IsNil("Expected transform runner to be created without error")

			entities := make([]*server.Entity, 0)
			entities = append(entities, server.NewEntity("1", 1))

			_, err = transform.transformEntities(&Runner{statsdClient: &statsd.NoOpClient{}}, entities)
			g.Assert(err).IsNotNil("tranform should fail")
			g.Assert(strings.Split(err.Error(), " (")[0]).
				Eql("ReferenceError: prefix is not defined at transform_entities")
		})
	})
}
