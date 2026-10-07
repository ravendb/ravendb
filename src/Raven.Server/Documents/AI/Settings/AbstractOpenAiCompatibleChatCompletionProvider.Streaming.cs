using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

// The streaming half of the OpenAI-compatible provider: SSE chunks, the streamed response and the provider's stream state.
internal abstract partial class AbstractOpenAiCompatibleChatCompletionProvider
{
    // The OpenAI-family state of one streamed response.
    protected sealed class OpenAiStreamState : ChatStreamState
    {
        public IToolCallState ToolCalls;

        // Refusal text is held apart from the answer: never streamed as content, never parsed as JSON.
        public StringBuilder RefusalText;
        public bool SawContent;
        public StringBuilder ReasoningFallback;     // reasoning is only ever a fallback, never streamed as the answer
        public StringBuilder RawText;               // the streamed text when StructuredOutput is false
    }

    public override ChatStreamState CreateStreamState() => new OpenAiStreamState { ToolCalls = CreateStreamToolCallState() };

    protected virtual IToolCallState CreateStreamToolCallState() => new ToolCallState();

    public override StreamEventResult ProcessStreamEvent(JsonOperationContext ctx, BlittableJsonReaderObject sseEvent, ChatStreamState chatState, AiUsage usage)
    {
        var state = (OpenAiStreamState)chatState;

        if (sseEvent.TryGet(ChatCompletionClient.Constants.ResponseFields.Usage, out BlittableJsonReaderObject streamedUsage) && streamedUsage is not null)
            usage.UpdateFrom(streamedUsage);

        if (sseEvent.TryGet(ChatCompletionClient.Constants.ResponseFields.Choices, out BlittableJsonReaderArray choices) is false || choices.Length == 0)
            return default;

        var choice = (BlittableJsonReaderObject)choices[0];

        var chunkFinishReason = GetFinishReason(choice);
        if (chunkFinishReason != null)
            state.StopReason = chunkFinishReason;

        // Probe the refusal on every choice, not only on chunks that carry a delta: Azure and Google signal it on
        // the choice itself (finish_reason / content_filter_results), possibly on a terminal chunk with no delta.
        var hasDelta = choice.TryGet(ChatCompletionClient.Constants.ResponseFields.Delta, out BlittableJsonReaderObject delta);

        var refusalDelta = GetRefusal(choice, delta, streaming: true, out var refusalIsComplete);
        if (string.IsNullOrEmpty(refusalDelta) == false)
        {
            // OpenAI streams the refusal as text fragments to concatenate; Azure and Google derive a full message per chunk
            // that may repeat and must be kept once. The text is kept apart from the answer so it is neither parsed as
            // JSON nor returned as content.
            if (refusalIsComplete)
                state.RefusalText ??= new StringBuilder(refusalDelta);
            else
                (state.RefusalText ??= new StringBuilder()).Append(refusalDelta);
        }

        if (hasDelta == false)
            return default;

        LazyStringValue textDelta = null;

        if (delta.TryGet(ChatCompletionClient.Constants.ResponseFields.Content, out LazyStringValue content) && content?.Length > 0)
        {
            state.ToolCalls.AddAndReset();
            state.SawContent = true;
            state.ReasoningFallback = null;
            textDelta = content;

            if (state.StructuredOutput == false)
                (state.RawText ??= new StringBuilder()).Append(content.ToString());
        }
        else if (TryGetDeltaReasoning(delta, out var reasoning))
        {
            state.ToolCalls.AddAndReset();

            // reasoning is held aside: it becomes the answer only if the model produced nothing else
            if (state.SawContent == false)
                (state.ReasoningFallback ??= new StringBuilder()).Append(reasoning);
        }

        if (delta.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray toolCalls))
        {
            foreach (BlittableJsonReaderObject toolCallChunk in toolCalls)
                state.ToolCalls.Merge(toolCallChunk);
        }

        return new StreamEventResult(textDelta, stop: false);
    }

    public override AiResponse BuildStreamedResponse(JsonOperationContext streamingCtx, ChatStreamState chatState, HttpResponseMessage response)
    {
        var state = (OpenAiStreamState)chatState;

        // [DONE] closes the tool call still being merged; a stream cut before [DONE] leaves it out.
        if (state.SawStop)
            state.ToolCalls.AddAndReset();

        var hasToolCalls = state.ToolCalls.TryGetToolCallsForMessage(out var allToolCalls);

        // Refusal first, then a token-limit cut, and both before returning tool calls: a filtered or cut stream can
        // carry partial tool-call deltas that must not reach the agent loop.
        if (state.RefusalText is { Length: > 0 })
            RefusedToAnswerException.Throw(state.RefusalText.ToString(), "[streaming]", state.StopReason, GetRequestId(response.Headers));

        var lengthTruncated = string.Equals(state.StopReason, ChatCompletionClient.Constants.ResponseFields.FinishReasonLength, StringComparison.OrdinalIgnoreCase);

        // Runs before tool calls are returned. Partial plain text is still an answer; a cut structured object or a cut
        // tool call is not.
        if (lengthTruncated && (state.StructuredOutput || hasToolCalls))
        {
            var what = hasToolCalls ? "tool call" : "structured answer";
            throw new TooManyTokensException(
                $"The model response was truncated (finish_reason='length') before producing a complete {what}.{ReasoningPreview(state)}")
            {
                RequestId = GetRequestId(response.Headers)
            };
        }

        if (hasToolCalls)
        {
            return new AiResponse(AiResponseType.Tool)
            {
                Message = CreateToolCallsMessage(streamingCtx, allToolCalls, "persisted/streamed/toolcalls"),
                ToolCalls = state.ToolCalls.GetAllToolCalls(),
            };
        }

        if (state.StructuredOutput == false)
        {
            if (state.SawContent == false && state.ReasoningFallback is { Length: > 0 })
            {
                state.PendingChunk = state.ReasoningFallback.ToString();
                (state.RawText ??= new StringBuilder()).Append(state.PendingChunk);
                state.ReasoningFallback = null;
            }

            var fullText = state.RawText?.ToString() ?? string.Empty;
            return new AiResponse(AiResponseType.Result)
            {
                Message = CreateAssistantMessage(streamingCtx, fullText, "persisted/streamed/message"),
                Result = fullText,
            };
        }

        if (state.FinalResult == null && state.SawContent == false && state.ReasoningFallback is { Length: > 0 } && state.Parser != null)
        {
            if (state.Parser.TryProcess(streamingCtx.GetLazyString(state.ReasoningFallback.ToString()), out var promoted) && promoted != null)
            {
                state.FinalResult = promoted;
                state.PendingChunk = string.Empty;
            }
        }

        if (state.FinalResult == null)
        {
            var problem = state.SawContent
                ? state.Parser is { IsInvalid: true }
                    ? "streamed content that is not valid JSON"
                    : "streamed content that did not form a complete JSON object"
                : state.ReasoningFallback is { Length: > 0 }
                    ? "streamed no content, and its reasoning is not the structured answer"
                    : "streamed no content";

            throw UnexpectedResponseException.Create(
                $"Structured output was requested but the model {problem} (finish_reason='{state.StopReason}').{ReasoningPreview(state)}",
                response, content: (string)null, GetRequestId(response.Headers));
        }

        return new AiResponse(AiResponseType.Result)
        {
            Message = CreateAssistantMessage(streamingCtx, state.FinalResult, "persisted/streamed/message"),
            Result = state.FinalResult,
        };
    }

    private static string ReasoningPreview(OpenAiStreamState state)
    {
        if (state.ReasoningFallback is not { Length: > 0 })
            return string.Empty;

        const int maxLength = 500;
        return state.ReasoningFallback.Length <= maxLength
            ? $" Reasoning: {state.ReasoningFallback}"
            : $" Reasoning: {state.ReasoningFallback.ToString(0, maxLength)}...";
    }
}
