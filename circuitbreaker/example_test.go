package circuitbreaker_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/tec-smith/microservices-orchestrator/circuitbreaker"
)

// Wrap any call in a breaker. After the configured number of consecutive
// failures, calls are rejected without running until Timeout has passed.
func ExampleBreaker_Execute() {
	cb := circuitbreaker.New(circuitbreaker.Settings{
		Name:        "payments",
		ReadyToTrip: circuitbreaker.ConsecutiveFailures(2),
		Timeout:     30 * time.Second,
	})
	charge := func() error { return errors.New("payment provider timed out") }

	for i := 1; i <= 3; i++ {
		err := cb.Execute(charge)
		fmt.Printf("call %d: %v (circuit %s)\n", i, err, cb.State())
	}
	// Output:
	// call 1: payment provider timed out (circuit closed)
	// call 2: payment provider timed out (circuit open)
	// call 3: circuitbreaker: circuit is open (circuit open)
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
