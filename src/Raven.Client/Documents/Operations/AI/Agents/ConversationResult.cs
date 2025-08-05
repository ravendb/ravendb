using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class ConversationResult<TAnswer>
{
    public string ConversationId { get; set; }
    public string ChangeVector { get; set; }
    public TAnswer Response { get; set; }
    public AiUsage TotalUsage { get; set; }
    public List<AiAgentActionRequest> ActionRequests { get; set; }

    internal static ConversationResult<TAnswer> Convert(BlittableJsonReaderObject response, DocumentConventions conventions)
    {
        response.TryGet(nameof(TotalUsage), out BlittableJsonReaderObject usage);
        response.TryGet(nameof(Response), out BlittableJsonReaderObject result);
        response.TryGet(nameof(ConversationId), out string conversationId);
        response.TryGet(nameof(ChangeVector), out string changeVector);

        List<AiAgentActionRequest> requests = null;
        if (response.TryGet(nameof(ActionRequests), out BlittableJsonReaderArray actionRequests) && actionRequests != null)
        {
            requests = [];
            foreach (BlittableJsonReaderObject actionRequest in actionRequests)
            {
                var r = JsonDeserializationClient.ActionRequest(actionRequest);
                requests.Add(r);
            }
        }

        return new ConversationResult<TAnswer>
        {
            ConversationId = conversationId,
            ChangeVector = changeVector,
            ActionRequests = requests,
            TotalUsage = JsonDeserializationClient.AiUsage(usage),
            Response = result == null ? default : conventions.Serialization.DefaultConverter.FromBlittable<TAnswer>(result, conversationId)
        };
    }
}
