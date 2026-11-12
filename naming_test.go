package solace_test

import (
	"strings"
	"testing"

	solace "code.internetisalie.net/watermill-solace"
)

func TestQueueNamers(t *testing.T) {
	upper := strings.ToUpper
	for _, tc := range []struct {
		name  string
		namer solace.QueueNamer
		topic string
		want  string
	}{
		{"topic default", solace.TopicQueueName, "orders/*/created/>", "orders/_STR_/created/_GT_"},
		{"group default", solace.GroupQueueName("billing"), "orders/*/created/>", "billing/orders/_STR_/created/_GT_"},
		{"group custom sanitizer", solace.GroupQueueNameWith("billing", upper), "orders/*", "billing/ORDERS/*"},
	} {
		if got := tc.namer(tc.topic); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
