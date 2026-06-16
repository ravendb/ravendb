using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Wizard "Test agent" input (design §1.3 Stage C.2 step 12). The operator tests the
/// <em>draft</em> configuration being edited in the Review step — before it is
/// provisioned — so the request carries the full <see cref="AiAgentConfiguration"/>
/// rather than a persisted agent id (which does not exist yet). Runs one
/// non-persisted turn against RavenDB's agent test endpoint.
/// </summary>
/// <param name="Prompt">The test prompt to send the agent.</param>
/// <param name="Configuration">The draft agent configuration to run the turn against.</param>
/// <param name="Parameters">Conversation-level parameter values the operator supplies
/// for the run (name -> value); optional.</param>
/// <param name="StreamField">Which output property streams token-by-token (the wizard's
/// "Streamed field" select). Optional — when unset, the conventional first-declared output
/// field is used (see <see cref="Raven.AiAppliance.Agents.AgentOutputShape.ResolveReplyField"/>).</param>
public sealed record SetupTryRequest(
    string Prompt,
    AiAgentConfiguration Configuration,
    Dictionary<string, string>? Parameters,
    string? StreamField = null);
