using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Client.Documents.AI;
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
    public AiUsage Usage { get; set; }
    public DateTime? Time { get; set; }
    public List<AiAgentActionRequest> ActionRequests { get; set; }

    internal static ConversationResult<TAnswer> Convert(BlittableJsonReaderObject response, DocumentConventions conventions)
    {
        response.TryGet(nameof(TotalUsage), out BlittableJsonReaderObject totalUsageBjo);
        response.TryGet(nameof(Response), out BlittableJsonReaderObject responseEnvelope);
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

        BlittableJsonReaderObject answerBjo = null;
        BlittableJsonReaderObject usageBjo = null;
        string timeStr = null;

        if (responseEnvelope != null)
        {
            responseEnvelope.TryGet(nameof(AiAnswer<object>.Answer), out answerBjo);
            responseEnvelope.TryGet(nameof(AiAnswer<object>.Usage), out usageBjo);
            responseEnvelope.TryGet(nameof(AiAnswer<object>.Time), out timeStr);
        }

        var result = new ConversationResult<TAnswer>
        {
            ConversationId = conversationId,
            ChangeVector = changeVector,
            ActionRequests = requests,
            TotalUsage = JsonDeserializationClient.AiUsage(totalUsageBjo),
            Response = answerBjo == null ? default : conventions.Serialization.DefaultConverter.FromBlittable<TAnswer>(answerBjo, conversationId),
            Usage = usageBjo == null ? new AiUsage() : JsonDeserializationClient.AiUsage(usageBjo),
            Time = string.IsNullOrEmpty(timeStr) ? null : DateTime.Parse(timeStr, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
        };
        return result;
    }
}
