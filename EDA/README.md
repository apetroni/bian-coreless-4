
# EDA prototype implementation of Party_Routing_Profile Service Domain for bian-coreless-4
## Architecture
- Party_Rouning_Profile service domain receives and persists flags related to various aspects about customers, incluging spend propensity, fraud and other indicators. The same service domain emits updates about customers' flags in real time to other service domains, publishing updates on BIAN EDA cloud. 

### Actors that interact with Party_Routing_Profile in coreless-v4
- Lead_Opportunity_Management service domain publishes updates about customers'spend propensity ratings  
- eBranch_And_Operations service domain receives customers' flag indicators' updates manageed and published by Party_Routing_Profile

### Implementation details
Party_Routing_Profiles consumes events from a persisted queue and emits events as publisher to a persistent destination. 
The BIAN EDA Cloud infra persists and rely messages to/from Party_Routing_Profile. The topic to queue mapping is managed at the broker side. 
A Solace cloud event broker is utilized.

Author: Alessandro Petroni apetroni@gmail.com apetroni@businessmesh.io
