// Package cluster contains parameters types for the cluster topology.
//
// Add fields to ClusterTopologyParameters and reference it via parametersSchema in
// topology.yaml when this topology needs custom parameters.
//
// +k8s:openapi-gen=true
package cluster

// ClusterTopologyParameters defines parameters for the cluster topology.
// Add fields here when the cluster topology needs custom parameters
// beyond what the base Instance spec provides.
//
// Example:
//
//	type ClusterTopologyParameters struct {
//	    NumShards int32 `json:"numShards,omitempty"`
//	}
//
// Then reference it in topology.yaml:
//
//	config:
//	  parametersSchema: ClusterTopologyParameters
type ClusterTopologyParameters struct{}
