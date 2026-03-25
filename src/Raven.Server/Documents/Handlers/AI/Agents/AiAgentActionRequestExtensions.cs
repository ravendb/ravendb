using System;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.AI;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal static class AiAgentActionRequestExtensions
{
    internal static bool IsInternalToolCall(this AiAgentActionRequest request)
    {
        if (request == null)
            throw new ArgumentNullException(nameof(request));

        return request.Name.Equals(ChatCompletionClient.Constants.ToolNames.RetrieveAttachment);
    }
}
