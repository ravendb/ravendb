using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

internal class ConversionRequestBody
{
    public List<AiAgentActionResponse> ActionResponses { get; set; }
    public List<AiAgentArtificialActionResponse> ArtificialActions { get; set; }

    public IEnumerable<ContentPart> UserPrompt { get; set; }
    public AiConversationCreationOptions CreationOptions { get; set; }
    public List<ICommandData> AttachmentCommands { get; set; }
    public AiOutputOptions OutputOptions { get; set; }

    public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
    {
        var json = new DynamicJsonValue
        {
            [nameof(ActionResponses)] = ActionResponses == null ? null : new DynamicJsonArray(ActionResponses.Select(r => r.ToJson())),
            [nameof(ArtificialActions)] = ArtificialActions == null ? null : new DynamicJsonArray(ArtificialActions.Select(r => r.ToJson())),
            [nameof(CreationOptions)] = (CreationOptions ?? new AiConversationCreationOptions()).ToJson(),
            [nameof(UserPrompt)] = UserPrompt == null ? null : new DynamicJsonArray(UserPrompt.Select(part => part.ToJson()))
        };

        if (OutputOptions != null)
            json[nameof(OutputOptions)] = OutputOptions.ToJson(conventions, context);

        return json;
    }
}
