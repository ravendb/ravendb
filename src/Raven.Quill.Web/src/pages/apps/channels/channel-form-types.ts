// The agent the channel routes to, when the caller has already chosen it (e.g. the capability
// wizard just created it). When omitted, the operator picks from the app's agents.
export type FixedAgent = { agentId: string; name: string };
