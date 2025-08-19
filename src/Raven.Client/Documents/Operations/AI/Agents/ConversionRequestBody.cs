using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.AI;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

internal class ConversionRequestBody : IDynamicJson
{
    public List<AiAgentActionResponse> ActionResponses { get; set; }
    public string UserPrompt { get; set; }
    public AiConversationCreationOptions CreationOptions { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ActionResponses)] = ActionResponses == null ? null : new DynamicJsonArray(ActionResponses.Select(r => r.ToJson())),
            [nameof(UserPrompt)] = UserPrompt,
            [nameof(CreationOptions)] = (CreationOptions ?? new AiConversationCreationOptions()).ToJson()
        };
    }
}
