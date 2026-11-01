package registry_test

import (
	"context"
	"fmt"
	"time"

	"github.com/tec-smith/microservices-orchestrator/registry"
)

// Register a service for as long as it runs. KeepAlive renews the lease
// every interval and deregisters when the context ends.
func ExampleKeepAlive() {
	reg := registry.NewMemory(registry.WithTTL(30 * time.Second))
	defer reg.Close()

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = registry.KeepAlive(ctx, reg, registry.Instance{
			ID:      "users-1",
			Service: "users",
			Address: "http://10.0.0.5:8080",
		}, 10*time.Second)
	}()

	for {
		if instances, _ := reg.Instances(ctx, "users"); len(instances) > 0 {
			fmt.Println("registered:", instances[0].ID, instances[0].Status)
			break
		}
		time.Sleep(time.Millisecond)
	}

	stop() // e.g. on SIGTERM
	<-done
	instances, _ := reg.Instances(context.Background(), "users")
	fmt.Println("instances after shutdown:", len(instances))
	// Output:
	// registered: users-1 passing
	// instances after shutdown: 0
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
