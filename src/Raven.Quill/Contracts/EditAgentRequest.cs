using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;

namespace Raven.Quill.Contracts;

public sealed record EditAgentRequest(
    AiAgentConfiguration Configuration,
    Dictionary<string, WebhookBinding>? ActionBindings);

public sealed record AgentDetailsResponse(
    AiAgentConfiguration Configuration,
    Dictionary<string, WebhookBinding> ActionBindings);
