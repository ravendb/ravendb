namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Wizard "Try the agent" smoke-test input (design §1.3 Stage C.2 step 12).
/// Streams a single turn against the per-app agent so the operator can confirm
/// it answers before wiring a channel to it.
/// </summary>
/// <param name="Prompt">The test prompt to send the agent.</param>
/// <param name="AgentId">The agent identifier to run — required (the operator
/// picks which agent to smoke-test).</param>
public sealed record SetupTryRequest(
    string Prompt,
    string AgentId);
