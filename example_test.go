package orchestrator_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"

	orchestrator "github.com/tec-smith/microservices-orchestrator"
	"github.com/tec-smith/microservices-orchestrator/registry"
)

// Call a service by name. The host of the URL is the service name; the
// client finds a healthy instance in the registry and sends the request there.
func ExampleNewClient() {
	users := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "user %s", r.URL.Path[len("/users/"):])
	}))
	defer users.Close()

	reg := registry.NewMemory()
	defer reg.Close()
	_ = reg.Register(context.Background(), registry.Instance{
		ID:      "users-1",
		Service: "users",
		Address: users.URL,
	})

	client := orchestrator.NewClient(reg)
	resp, err := client.Get("http://users/users/42")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
	// Output: user 42
}

// Put the gateway in front of the registry, so any HTTP client can reach a
// service at /{service}/{path}.
func ExampleNewGateway() {
	orders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "orders saw %s", r.URL.Path)
	}))
	defer orders.Close()

	reg := registry.NewMemory()
	defer reg.Close()
	_ = reg.Register(context.Background(), registry.Instance{
		ID:      "orders-1",
		Service: "orders",
		Address: orders.URL,
	})

	gateway := httptest.NewServer(orchestrator.NewGateway(orchestrator.NewTransport(reg), nil))
	defer gateway.Close()

	resp, err := http.Get(gateway.URL + "/orders/api/v1/orders")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
	// Output: orders saw /api/v1/orders
}

/***
  @COPYRIGHT (c) 2026. Thomas EC. Smith (https://www.TECSmith.info). Licensed under the MIT License.
****/
