using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Exceptions;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.AI.Settings;

// The streaming half of the Anthropic provider: SSE events, the streamed response and the provider's stream state.
internal sealed partial class AnthropicChatCompletionProvider
{
    public override StreamEventResult ProcessStreamEvent(JsonOperationContext ctx, BlittableJsonReaderObject sseEvent, ChatStreamState state, AiUsage usage)
    {
        var progress = AsStreamState<AnthropicStreamState>(state);
        var blocks = progress.Blocks;

        if (sseEvent.TryGet(Wire.Type, out string eventType) == false)
            return default;

        switch (eventType)
        {
            case Wire.EventMessageStart:
                if (sseEvent.TryGet(Wire.Message, out BlittableJsonReaderObject startMessage) && startMessage != null &&
                    startMessage.TryGet(Wire.Usage, out BlittableJsonReaderObject startUsage) && startUsage != null)
                {
                    startUsage.TryGet(Wire.InputTokens, out long inputTokens);
                    startUsage.TryGet(Wire.CacheReadInputTokens, out long cacheRead);
                    startUsage.TryGet(Wire.CacheCreationInputTokens, out long cacheCreation);
                    UpdateUsage(usage, promptTokens: inputTokens + cacheRead + cacheCreation, completionTokens: 0, cachedTokens: cacheRead, reasoningTokens: 0);
                }
                return default;

            case Wire.EventContentBlockStart:
                if (sseEvent.TryGet(Wire.Index, out int startIndex) &&
                    sseEvent.TryGet(Wire.ContentBlock, out BlittableJsonReaderObject cb) && cb != null)
                {
                    cb.TryGet(Wire.Type, out string blockType);
                    var block = new StreamingBlock { Type = blockType };
                    cb.TryGet(Wire.Id, out block.Id);
                    cb.TryGet(Wire.Name, out block.Name);
                    if (cb.TryGet(Wire.Text, out string seedText) && string.IsNullOrEmpty(seedText) == false)
                        block.Text.Append(seedText);
                    blocks[startIndex] = block;
                }
                return default;

            case Wire.EventContentBlockDelta:
                if (sseEvent.TryGet(Wire.Index, out int deltaIndex) &&
                    sseEvent.TryGet(Wire.Delta, out BlittableJsonReaderObject delta) && delta != null)
                {
                    delta.TryGet(Wire.Type, out string deltaType);
                    blocks.TryGetValue(deltaIndex, out var block);

                    switch (deltaType)
                    {
                        case Wire.DeltaText:
                            delta.TryGet(Wire.Text, out LazyStringValue textChunk);
                            if (textChunk == null || textChunk.Length == 0 || block == null)
                                return default;

                            block.Text.Append(textChunk.ToString());

                            if (state.StructuredOutput == false)
                                return new StreamEventResult(textChunk, stop: false);

                            if (block.IsAnswerJson == null)
                            {
                                var first = FirstNonWhitespace(block.Text);
                                if (first != '\0')
                                    block.IsAnswerJson = first == '{';
                            }

                            return block.IsAnswerJson == true ? new StreamEventResult(textChunk, stop: false) : default;

                        case Wire.DeltaInputJson:
                            delta.TryGet(Wire.PartialJson, out string partial);
                            block?.Json.Append(partial);
                            return default;
                    }
                }
                return default;

            case Wire.EventMessageDelta:
                if (sseEvent.TryGet(Wire.Delta, out BlittableJsonReaderObject messageDelta) && messageDelta != null)
                {
                    if (messageDelta.TryGet(Wire.StopReason, out string stopReason) && stopReason != null)
                        state.StopReason = stopReason;
                }
                if (sseEvent.TryGet(Wire.Usage, out BlittableJsonReaderObject deltaUsage) && deltaUsage != null)
                {
                    // The counts are cumulative and a stream may carry more than one message_delta, so only the last one is
                    // kept and it is counted once, on message_stop.
                    deltaUsage.TryGet(Wire.OutputTokens, out progress.OutputTokens);
                    progress.ThinkingTokens = ThinkingTokens(deltaUsage);
                }
                return default;

            case Wire.EventMessageStop:
                UpdateUsage(usage, promptTokens: 0, completionTokens: progress.OutputTokens, cachedTokens: 0, reasoningTokens: progress.ThinkingTokens);
                return new StreamEventResult(null, stop: true);

            case Wire.EventError:
                throw BuildMidStreamError(sseEvent, state);

            default:
                // "ping" and any unknown event types are intentionally ignored (forward-compatible).
                return default;
        }
    }

    public override AiResponse BuildStreamedResponse(JsonOperationContext streamingCtx, ChatStreamState state, HttpResponseMessage response)
    {
        if (state.SawStop == false)
            throw UnexpectedResponseException.Create("The stream ended before the provider signaled completion (message_stop); the response is incomplete",
                response, string.Empty, GetRequestId(response.Headers));

        // Partial plain text is still an answer; a cut structured object or a cut tool call is not.
        var lengthTruncated = state.StopReason is Wire.StopReasonMaxTokens or Wire.StopReasonModelContextWindowExceeded;
        if (lengthTruncated && state.StructuredOutput)
            throw new TooManyTokensException($"The model stopped because it ran out of room (stop_reason='{state.StopReason}').") { RequestId = GetRequestId(response.Headers) };

        var blocks = AsStreamState<AnthropicStreamState>(state).Blocks;
        var ordered = blocks.OrderBy(kv => kv.Key).Select(kv => kv.Value).ToList();
        var hasToolUse = ordered.Any(b => b.Type == Wire.TypeToolUse);

        if (state.StopReason == Wire.StopReasonRefusal)
        {
            var refusalText = string.Concat(ordered.Where(b => b.Type == Wire.TypeText).Select(b => b.Text.ToString()));
            RefusedToAnswerException.Throw(RefusalText(refusalText), "[streaming]", state.StopReason, GetRequestId(response.Headers));
        }

        if (lengthTruncated && hasToolUse)
            throw new TooManyTokensException($"The model stopped because it ran out of room (stop_reason='{state.StopReason}').") { RequestId = GetRequestId(response.Headers) };

        if (hasToolUse == false)
        {
            if (state.StructuredOutput == false)
            {
                var plainText = string.Concat(ordered.Where(b => b.Type == Wire.TypeText).Select(b => b.Text.ToString()));
                if (lengthTruncated && plainText.Length == 0)
                    throw new TooManyTokensException($"The model stopped because it ran out of room (stop_reason='{state.StopReason}').") { RequestId = GetRequestId(response.Headers) };

                return new AiResponse(AiResponseType.Result)
                {
                    Result = plainText,
                    Message = CreateAssistantMessage(streamingCtx, plainText, "anthropic/streamed/text")
                };
            }

            var resultMessage = state.FinalResult;
            if (resultMessage == null)
            {
                var text = string.Concat(ordered.Where(b => b.Type == Wire.TypeText).Select(b => b.Text.ToString()));
                if (string.IsNullOrEmpty(text))
                    throw UnexpectedResponseException.Create("No content in streamed response", response, string.Empty, GetRequestId(response.Headers));

                try
                {
                    resultMessage = streamingCtx.Sync.ReadForMemory(text, "ai/output");
                }
                catch (Exception e) when (e is InvalidDataException or InvalidStartOfObjectException or EndOfStreamException)
                {
                    throw UnexpectedResponseException.Create(
                        $"Structured output was requested but the model streamed content that is not valid JSON (stop_reason='{state.StopReason}'). Content: {text}",
                        response, string.Empty, GetRequestId(response.Headers));
                }
            }

            return new AiResponse(AiResponseType.Result)
            {
                Result = resultMessage,
                Message = CreateAssistantMessage(streamingCtx, resultMessage, "anthropic/streamed/result")
            };
        }

        var toolCalls = new List<AiToolCall>();
        foreach (var block in ordered)
        {
            if (block.Type == Wire.TypeToolUse)
                toolCalls.Add(new AiToolCall(block.Id, block.Name, block.Json.Length > 0 ? block.Json.ToString() : "{}"));
        }

        return new AiResponse(AiResponseType.Tool)
        {
            ToolCalls = toolCalls,
            Message = CreateToolCallsMessage(streamingCtx, ToToolCallsArray(toolCalls), "anthropic/streamed/tool")
        };
    }

    public override ChatStreamState CreateStreamState() => new AnthropicStreamState();

    private sealed class AnthropicStreamState : ChatStreamState
    {
        public readonly Dictionary<int, StreamingBlock> Blocks = new();
        public long OutputTokens;
        public long ThinkingTokens;
    }

    private sealed class StreamingBlock
    {
        public string Type;
        public string Id;
        public string Name;

        // Whether this text block carries the structured answer. Null until the first non-whitespace character.
        public bool? IsAnswerJson;

        public readonly StringBuilder Text = new();
        public readonly StringBuilder Json = new();
    }

    private static char FirstNonWhitespace(StringBuilder text)
    {
        for (var i = 0; i < text.Length; i++)
        {
            if (char.IsWhiteSpace(text[i]) == false)
                return text[i];
        }

        return '\0';
    }

    private UnsuccessfulAiRequestException BuildMidStreamError(BlittableJsonReaderObject sseEvent, ChatStreamState state)
    {
        var (errorType, errorMessage) = ExtractError(sseEvent);
        var requestId = GetRequestId(state.Response.Headers);
        var message = $"The model returned an error mid-stream (type: '{errorType ?? "unknown"}'): {errorMessage ?? sseEvent.ToString()}";

        // The stream opened with a 200, which carries no Retry-After: back off like a 429 without one.
        if (errorType == Wire.ErrorRateLimit)
            return new InsufficientQuotaException(message) { RequestId = requestId };

        var status = errorType switch
        {
            Wire.ErrorOverloaded => (HttpStatusCode)529,
            Wire.ErrorApi => HttpStatusCode.InternalServerError,
            Wire.ErrorInvalidRequest => HttpStatusCode.BadRequest,
            Wire.ErrorAuthentication => HttpStatusCode.Unauthorized,
            Wire.ErrorPermission => HttpStatusCode.Forbidden,
            Wire.ErrorNotFound => HttpStatusCode.NotFound,
            Wire.ErrorRequestTooLarge => HttpStatusCode.RequestEntityTooLarge,
            _ => HttpStatusCode.InternalServerError
        };

        return new UnsuccessfulAiRequestException(message, status)
        {
            RequestId = requestId
        };
    }
}
