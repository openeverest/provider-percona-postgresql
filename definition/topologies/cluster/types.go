// Package cluster contains custom spec types for the cluster topology.
//
// Add fields to ClusterTopologyConfig and reference it via configSchema in
// topology.yaml when this topology needs custom configuration.
//
// +k8s:openapi-gen=true
package cluster

// ClusterTopologyConfig defines configuration for the cluster topology.
// Add fields here when the cluster topology needs custom configuration
// beyond what the base Instance spec provides.
//
// Example:
//
//	type ClusterTopologyConfig struct {
//	    NumShards int32 `json:"numShards,omitempty"`
//	}
//
// Then reference it in topology.yaml:
//
//	config:
//	  configSchema: ClusterTopologyConfig
type ClusterTopologyConfig struct{}
