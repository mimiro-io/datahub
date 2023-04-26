// Copyright 2021 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobs

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/franela/goblin"
	"go.uber.org/zap"
)

func TestRaffle(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The raffle", func() {
		g.It("Should only give out one ticker per job", func() {
			raffle := NewRaffle(0, 2, zap.NewNop().Sugar(), &statsd.NoOpClient{})
			job := &job{id: "test1", pipeline: &IncrementalPipeline{}}

			ticket := raffle.borrowTicket(job)
			g.Assert(ticket).IsNotZero("Expected to get ticket")

			ticket2 := raffle.borrowTicket(job)
			g.Assert(ticket2).IsZero("Expected ticket to be taken")
		})

		g.It("Should not exceed ticket limit", func() {
			raffle := NewRaffle(0, 1, zap.NewNop().Sugar(), &statsd.NoOpClient{})
			job1 := &job{id: "test1", pipeline: &IncrementalPipeline{}}

			ticket := raffle.borrowTicket(job1)
			g.Assert(ticket).IsNotZero("Expected to get ticket")

			job2 := &job{id: "test2", pipeline: &IncrementalPipeline{}}
			ticket2 := raffle.borrowTicket(job2)
			g.Assert(ticket2).IsZero("Expected limit to be respected")
		})

		g.It("Should give out ticket again after it was returned", func() {
			raffle := NewRaffle(0, 1, zap.NewNop().Sugar(), &statsd.NoOpClient{})
			job1 := &job{id: "test1", pipeline: &IncrementalPipeline{}}

			ticket := raffle.borrowTicket(job1)
			g.Assert(ticket).IsNotZero("Expected to get ticket")
			raffle.returnTicket(ticket)

			job2 := &job{id: "test2", pipeline: &IncrementalPipeline{}}
			ticket2 := raffle.borrowTicket(job2)
			g.Assert(ticket2).IsNotZero("Expected to get ticket")
		})
		g.It("Should not get confused about tickets in concurrent situations", func() {
			raffle := NewRaffle(0, 3, zap.NewNop().Sugar(), &statsd.NoOpClient{})

			var wg sync.WaitGroup
			running := 0

			// start 30 jobs
			for i := 0; i < 30; i++ {
				if running == 3 {
					// make sure we only start 3 at a time by using a wait-group
					wg.Wait()
					running = 0
				} else {
					wg.Add(1)
					running = running + 1
					id := strconv.Itoa(i)
					go func() {
						job := &job{id: "test" + id, pipeline: &IncrementalPipeline{}}
						ticket := raffle.borrowTicket(job)
						g.Assert(ticket).IsNotZero("Expected to get ticket for job " + id)
						// simulate work
						time.Sleep(10 * time.Millisecond)
						raffle.returnTicket(ticket)
						wg.Done()
					}()
				}
			}
		})
	})
}
