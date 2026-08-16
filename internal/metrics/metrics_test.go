package metrics

import (
	"fmt"
	"testing"
)

// TestBoundedDomain guards OBS-04's own cardinality warning: "domain" is
// derived from scrape-target URLs (attacker/operator-influenced input), so
// without a cap a feed of distinct domains would grow Prometheus label
// cardinality without bound.
func TestBoundedDomain(t *testing.T) {
	seenDomains = make(map[string]struct{}, maxDomainLabels)

	for i := 0; i < maxDomainLabels; i++ {
		domain := fmt.Sprintf("site-%d.example", i)
		if got := BoundedDomain(domain); got != domain {
			t.Fatalf("expected domain %d to keep its own label, got %q", i, got)
		}
	}

	if got := BoundedDomain("overflow.example"); got != "other" {
		t.Errorf("expected the %dth distinct domain to collapse to \"other\", got %q", maxDomainLabels+1, got)
	}

	// A domain already seen before the cap was hit must keep its own label.
	if got := BoundedDomain("site-0.example"); got != "site-0.example" {
		t.Errorf("expected a previously-seen domain to keep its own label, got %q", got)
	}
}
