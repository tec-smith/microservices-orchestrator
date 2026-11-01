// Command client calls a service by name using client-side discovery: it
// asks the registry for instances and balances requests itself, with no
// gateway in between.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	orchestrator "github.com/tec-smith/microservices-orchestrator"
	"github.com/tec-smith/microservices-orchestrator/loadbalancer"
	"github.com/tec-smith/microservices-orchestrator/registry"
)

func main() {
	registryURL := flag.String("registry", "http://localhost:8500", "registry base URL")
	service := flag.String("service", "echo", "service to call")
	n := flag.Int("n", 10, "number of requests")
	strategy := flag.String("lb", loadbalancer.StrategyRoundRobin, "load balancing strategy")
	flag.Parse()

	balancer, err := loadbalancer.New(*strategy)
	if err != nil {
		log.Fatal(err)
	}
	client := orchestrator.NewClient(registry.NewHTTPClient(*registryURL), orchestrator.WithBalancer(balancer))
	client.Timeout = 5 * time.Second

	for i := 1; i <= *n; i++ {
		resp, err := client.Get(fmt.Sprintf("http://%s/hello", *service))
		if err != nil {
			fmt.Printf("%2d  error: %v\n", i, err)
			continue
		}
		var body struct {
			Instance string `json:"instance"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		fmt.Printf("%2d  %s  %s\n", i, resp.Status, body.Instance)
	}
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
