using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Represents the result of running a conversation step with an AI agent.
/// Contains the model response (typed), usage counters, and any tool action requests.
/// </summary>
public class ConversationResult<TAnswer>
{
    /// <summary>
    /// The identifier of the conversation document used to persist the chat state.
    /// </summary>
    public string ConversationId { get; set; }
    /// <summary>
    /// The current change vector of the conversation document.
    /// </summary>
    public string ChangeVector { get; set; }
    /// <summary>
    /// The AI model response, deserialized to the requested type.
    /// </summary>
    public TAnswer Response { get; set; }
    /// <summary>
    /// Aggregated token usage counters for the conversation step.
    /// </summary>
    public AiUsage TotalUsage { get; set; }
    public AiUsage Usage { get; set; }
    public TimeSpan Elapsed { get; set; }
    /// <summary>
    /// Any tool action requests emitted by the model that require user/tool responses.
    /// </summary>
    public List<AiAgentActionRequest> ActionRequests { get; set; }
    internal int ToolsIterations { get; set; }

    internal static ConversationResult<TAnswer> Convert(BlittableJsonReaderObject response, DocumentConventions conventions)
    {
        response.TryGet(nameof(TotalUsage), out BlittableJsonReaderObject totalUsage);
        response.TryGet(nameof(Response), out BlittableJsonReaderObject resultBjo);
        response.TryGet(nameof(ConversationId), out string conversationId);
        response.TryGet(nameof(ChangeVector), out string changeVector);
        response.TryGet(nameof(Usage), out BlittableJsonReaderObject usage);
        response.TryGet(nameof(Elapsed), out TimeSpan elapsedStr);

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
            TotalUsage = JsonDeserializationClient.AiUsage(totalUsage),
            Response = resultBjo == null ? default : conventions.Serialization.DefaultConverter.FromBlittable<TAnswer>(resultBjo, conversationId),
            Usage = JsonDeserializationClient.AiUsage(usage),
            Elapsed = elapsedStr
        };
    }
}
