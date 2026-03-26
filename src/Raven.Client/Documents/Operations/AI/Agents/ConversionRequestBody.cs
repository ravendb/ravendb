using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.AI;
using Sparrow.Json.Parsing;
using Raven.Client.Documents.Commands.Batches;

namespace Raven.Client.Documents.Operations.AI.Agents;

internal class ConversionRequestBody : IDynamicJson
{
    public List<AiAgentActionResponse> ActionResponses { get; set; }
    public List<AiAgentArtificialActionResponse> ArtificialActions { get; set; }

    public IEnumerable<ContentPart> UserPrompt { get; set; }
    public AiConversationCreationOptions CreationOptions { get; set; }
    public List<ICommandData> AttachmentCommands { get; set; }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(ActionResponses)] = ActionResponses == null ? null : new DynamicJsonArray(ActionResponses.Select(r => r.ToJson())),
            [nameof(ArtificialActions)] = ArtificialActions == null ? null : new DynamicJsonArray(ArtificialActions.Select(r => r.ToJson())),
            [nameof(CreationOptions)] = (CreationOptions ?? new AiConversationCreationOptions()).ToJson(),
            [nameof(UserPrompt)] = UserPrompt == null ? null : new DynamicJsonArray(UserPrompt.Select(part => part.ToJson()))
        };

        return json;
    }
}
