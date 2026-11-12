using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

// The streaming half of the Responses provider: SSE events, the streamed response and the provider's stream state.
// Every event carries its own "type"; there is no [DONE], so the stream ends on response.completed or response.incomplete.
internal abstract partial class AbstractOpenAiResponsesProvider
{
    protected sealed class ResponsesStreamState : ChatStreamState
    {
        public readonly StringBuilder RawText = new();          // the streamed text when StructuredOutput is false
        public readonly StringBuilder RefusalText = new();      // never streamed as content, never parsed as JSON
        public string ProviderRefusal;                          // the first non-null GetProviderRefusal result
        public bool SawContent;
        public readonly List<StreamedFunctionCall> Calls = new(); // in output order, found by ItemId
    }

    protected sealed class StreamedFunctionCall
    {
        public string ItemId;                                   // "fc_..."; argument deltas refer to it
        public string CallId;                                   // "call_..."; becomes AiToolCall.Id
        public string Name;
        public readonly StringBuilder Arguments = new();
        public bool Done;                                       // response.output_item.done seen
    }

    public override ChatStreamState CreateStreamState() => new ResponsesStreamState();

    public override StreamEventResult ProcessStreamEvent(JsonOperationContext ctx, BlittableJsonReaderObject sseEvent, ChatStreamState chatState, AiUsage usage)
    {
        var state = AsStreamState<ResponsesStreamState>(chatState);

        if (sseEvent.TryGet(Wire.Type, out string eventType) == false)
            return default;

        switch (eventType)
        {
            case Wire.EventOutputItemAdded:
                if (TryGetFunctionCallItem(sseEvent, out var addedItem))
                {
                    addedItem.TryGet(Wire.Id, out string itemId);
                    var call = FindOrAddCall(state, itemId);
                    addedItem.TryGet(Wire.CallId, out call.CallId);
                    addedItem.TryGet(Wire.Name, out call.Name);
                }
                return default;

            case Wire.EventOutputTextDelta:
                if (sseEvent.TryGet(Wire.Delta, out LazyStringValue textDelta) == false || textDelta == null || textDelta.Length == 0)
                    return default;

                state.SawContent = true;

                if (state.StructuredOutput == false)
                    state.RawText.Append(textDelta.ToString());

                return new StreamEventResult(textDelta, stop: false);

            case Wire.EventRefusalDelta:
                if (sseEvent.TryGet(Wire.Delta, out string refusalDelta))
                    state.RefusalText.Append(refusalDelta);
                return default;

            case Wire.EventFunctionCallArgumentsDelta:
                if (sseEvent.TryGet(Wire.ItemId, out string argumentsItemId) && sseEvent.TryGet(Wire.Delta, out string argumentsDelta))
                    FindOrAddCall(state, argumentsItemId).Arguments.Append(argumentsDelta);
                return default;

            case Wire.EventOutputItemDone:
                if (TryGetFunctionCallItem(sseEvent, out var doneItem))
                {
                    doneItem.TryGet(Wire.Id, out string itemId);
                    var call = FindOrAddCall(state, itemId);

                    // the full item replaces what the deltas built
                    if (doneItem.TryGet(Wire.CallId, out string callId))
                        call.CallId = callId;
                    if (doneItem.TryGet(Wire.Name, out string name))
                        call.Name = name;
                    if (doneItem.TryGet(Wire.Arguments, out string arguments))
                        call.Arguments.Clear().Append(arguments);

                    call.Done = true;
                }
                return default;

            case Wire.EventCreated:
            case Wire.EventInProgress:
                CaptureProviderRefusal(state, StreamedResponse(sseEvent));
                return default;

            case Wire.EventCompleted:
                var completed = StreamedResponse(sseEvent);
                UpdateStreamUsage(completed, usage);
                CaptureProviderRefusal(state, completed);
                state.StopReason = Wire.StatusCompleted;
                return new StreamEventResult(null, stop: true);

            case Wire.EventIncomplete:
                var incomplete = StreamedResponse(sseEvent);
                UpdateStreamUsage(incomplete, usage);
                CaptureProviderRefusal(state, incomplete);
                state.StopReason = incomplete == null ? Wire.StatusIncomplete : IncompleteReason(incomplete) ?? Wire.StatusIncomplete;
                return new StreamEventResult(null, stop: true);

            case Wire.EventFailed:
                var failed = StreamedResponse(sseEvent);
                UpdateStreamUsage(failed, usage);
                throw UnexpectedResponseException.Create($"The model response failed mid-stream: {(failed == null ? "no error details" : ErrorMessage(failed))}",
                    state.Response, sseEvent, GetRequestId(state.Response.Headers));

            case Wire.EventError:
                sseEvent.TryGet(Wire.Code, out string code);
                sseEvent.TryGet(Wire.Message, out string message);
                throw UnexpectedResponseException.Create($"The model returned an error mid-stream (code: '{code ?? "unknown"}'): {message ?? sseEvent.ToString()}",
                    state.Response, sseEvent, GetRequestId(state.Response.Headers));

            default:
                // any other event type is ignored (forward-compatible)
                return default;
        }
    }

    public override AiResponse BuildStreamedResponse(JsonOperationContext streamingCtx, ChatStreamState chatState, HttpResponseMessage response)
    {
        var state = AsStreamState<ResponsesStreamState>(chatState);
        var requestId = GetRequestId(response.Headers);

        if (state.SawStop == false)
            throw UnexpectedResponseException.Create("The stream ended before the provider signaled completion (response.completed or response.incomplete); the response is incomplete",
                response, string.Empty, requestId);

        if (state.RefusalText.Length > 0)
            RefusedToAnswerException.Throw(state.RefusalText.ToString(), "[streaming]", state.StopReason, requestId);

        if (state.ProviderRefusal != null)
            RefusedToAnswerException.Throw(state.ProviderRefusal, "[streaming]", state.StopReason, requestId);

        if (state.StopReason == Wire.ReasonContentFilter)
            RefusedToAnswerException.Throw(ContentFilterRefusal, "[streaming]", state.StopReason, requestId);

        // Partial plain text is still an answer; a cut structured object or a cut tool call is not.
        var truncated = state.StopReason == Wire.ReasonMaxOutputTokens;
        if (truncated && (state.StructuredOutput || state.Calls.Count > 0 || state.RawText.Length == 0))
            throw Truncated(requestId, content: null);

        if (state.Calls.Count > 0)
        {
            var toolCalls = new List<AiToolCall>(state.Calls.Count);
            foreach (var call in state.Calls)
            {
                if (call.Done == false)
                    throw UnexpectedResponseException.Create($"The stream ended with an incomplete function call (item '{call.ItemId}', name '{call.Name}')",
                        response, string.Empty, requestId);

                toolCalls.Add(new AiToolCall(call.CallId, call.Name, call.Arguments.Length > 0 ? call.Arguments.ToString() : "{}"));
            }

            return new AiResponse(AiResponseType.Tool)
            {
                ToolCalls = toolCalls,
                Message = CreateToolCallsMessage(streamingCtx, ToToolCallsArray(toolCalls), "responses/streamed/tool")
            };
        }

        if (state.StructuredOutput == false)
        {
            var text = state.RawText.ToString();
            return new AiResponse(AiResponseType.Result)
            {
                Result = text,
                Message = CreateAssistantMessage(streamingCtx, text, "responses/streamed/text")
            };
        }

        if (state.FinalResult == null)
        {
            var problem = state.SawContent
                ? state.Parser is { IsInvalid: true }
                    ? "streamed content that is not valid JSON"
                    : "streamed content that did not form a complete JSON object"
                : "streamed no content";

            throw UnexpectedResponseException.Create($"Structured output was requested but the model {problem} (stop reason '{state.StopReason}')",
                response, content: (string)null, requestId);
        }

        return new AiResponse(AiResponseType.Result)
        {
            Result = state.FinalResult,
            Message = CreateAssistantMessage(streamingCtx, state.FinalResult, "responses/streamed/result")
        };
    }

    private static bool TryGetFunctionCallItem(BlittableJsonReaderObject sseEvent, out BlittableJsonReaderObject item)
    {
        if (sseEvent.TryGet(Wire.Item, out item) && item != null &&
            item.TryGet(Wire.Type, out string itemType) && itemType == Wire.TypeFunctionCall)
            return true;

        item = null;
        return false;
    }

    private void CaptureProviderRefusal(ResponsesStreamState state, BlittableJsonReaderObject streamedResponse)
    {
        if (streamedResponse == null || state.ProviderRefusal != null)
            return;

        state.ProviderRefusal = GetProviderRefusal(streamedResponse);
    }

    private static BlittableJsonReaderObject StreamedResponse(BlittableJsonReaderObject sseEvent) =>
        sseEvent.TryGet(Wire.Response, out BlittableJsonReaderObject response) ? response : null;

    private static void UpdateStreamUsage(BlittableJsonReaderObject response, AiUsage usage)
    {
        if (response != null && response.TryGet(Wire.Usage, out BlittableJsonReaderObject usageJson))
            UpdateUsage(usageJson, usage);
    }

    private static StreamedFunctionCall FindOrAddCall(ResponsesStreamState state, string itemId)
    {
        foreach (var call in state.Calls)
        {
            if (call.ItemId == itemId)
                return call;
        }

        var added = new StreamedFunctionCall { ItemId = itemId };
        state.Calls.Add(added);
        return added;
    }
}
