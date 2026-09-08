# Fabric (IOA) Integration Plan

## Architectural Shift: Data-Flow over State Machine
Entourage is evolving. While the Control-by-Return (CbR) state machine remains the perfect engine for *intra-agent* execution (tool loops, retries, crash recovery), *inter-agent* orchestration is moving to a **data-flow** model. 

Instead of declaring rigid cross-agent execution DAGs (the LangGraph approach), agents are standalone resident entities that communicate asynchronously via mailboxes. The orchestration graph is not declared upfront; it is an emergent trace on the Aethera/Fabric coordination plane.

## Integration Objectives
Replace the centralized Redis backend for queues and mailboxes with the decentralized `fabric-py` IOA daemon, aligning Entourage directly with ADR 0015.

### 1. The Coordination Plane (Ready-Queues & Mailboxes)
- [ ] **Create `AetheraFabricBackend`**: Implement a new backend in `entourage.runtime` alongside Redis and Memory.
- [ ] **Ready-Queue as Fabric Queue**: Map the worker ready-queue to Fabric's `Queue.claim()` and `Queue.ack()`. 
- [ ] **Mailboxes as Fabric Queues**: Map `WaitForMailbox` to Fabric's `Queue.wait()` and `claim()`.
- [ ] **Event Envelope Mapping**: Align Entourage's internal event schema with Fabric's `Event` dataclass (`kind`, `source`, `reply_target`, `correlation_id`).
- [ ] **Ingress Policy**: Leverage Fabric's `Queue.host(accepts=[...])` to enforce capability boundaries at the daemon level.

### 2. The Graph-Store (State Commit)
*Open Challenge: Fabric's `Bucket` currently lacks Compare-and-Swap (CAS) or transactional primitives needed for Entourage's atomic `Transition` commits.*
- [ ] **Decide Graph-Store Strategy**:
    - *Option A*: Keep the execution graph state in a dedicated store (e.g., SQLite, Redis, or Vault) while moving all coordination/queues to Fabric.
    - *Option B*: Extend Fabric's `Bucket` to support conditional updates/CAS, allowing the entire `RuntimeBackendConfig` to live on Fabric.

### 3. Subagent & Supervision Primitives
- [ ] **Subagent Spawning**: Update the spawn mechanic to provision a new Fabric identity (Queue) rather than splicing a sub-graph into the parent's CbR tree.
- [ ] **Status Registers**: Implement latest-value status registers (using Fabric Buckets or Vault) so supervising agents can check child status without flooding the mailbox.
- [ ] **Monitors (Liveness)**: Implement a mechanism where a crashed/timed-out child emits a `kind: system` death notice to the parent's Fabric Queue.
