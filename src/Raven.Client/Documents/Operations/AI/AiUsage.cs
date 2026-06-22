using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Tracks token usage for AI operations (prompt, completion, total and cached tokens).
/// </summary>
public class AiUsage : IDynamicJson
{
    /// <summary>Total number of tokens used in prompts.</summary>
    public long PromptTokens { get; set; }
    /// <summary>Total number of tokens produced by completions.</summary>
    public long CompletionTokens { get; set; }
    /// <summary>Total number of tokens used (prompt + completion).</summary>
    public long TotalTokens { get; set; }
    /// <summary>Number of tokens served from cache (e.g., embeddings cache), if available.</summary>
    public long CachedTokens { get; set; }
    /// <summary>The part of the completion tokens used for reasoning by the model.</summary>
    public long ReasoningTokens { get; set; }

    internal void UpdateFrom(BlittableJsonReaderObject json)
    {
        json.TryGet("prompt_tokens", out long promptTokens);
        json.TryGet("completion_tokens", out long completionTokens);
        json.TryGet("total_tokens", out long totalTokens);

        PromptTokens += promptTokens;
        CompletionTokens += completionTokens;
        TotalTokens += totalTokens;

        if (json.TryGet("prompt_tokens_details", out BlittableJsonReaderObject promptDetails) && promptDetails != null)
        {
            if (promptDetails.TryGet("cached_tokens", out long cachedTokens))
                CachedTokens += cachedTokens;
        }

        if (json.TryGet("completion_tokens_details", out BlittableJsonReaderObject completionTokensDetails) && completionTokensDetails != null)
        {
            if (completionTokensDetails.TryGet("reasoning_tokens", out long reasoningTokens))
            {
                ReasoningTokens += reasoningTokens;
            }
        }
    }

    /// <summary>
    /// Serializes the usage counters to JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(PromptTokens)] = PromptTokens,
            [nameof(CompletionTokens)] = CompletionTokens,
            [nameof(TotalTokens)] = TotalTokens,
            [nameof(CachedTokens)] = CachedTokens,
            [nameof(ReasoningTokens)] = ReasoningTokens
        };
    }

    internal static AiUsage GetUsageDifference(AiUsage current, AiUsage previous)
    {
        var previousTotalWithoutReasoning = (previous.CompletionTokens - previous.ReasoningTokens + previous.PromptTokens);
        return new AiUsage
        {
            PromptTokens = Math.Max(current.PromptTokens - previousTotalWithoutReasoning, 0), // in case the model gives us crappy results and current.PromptTokens - previousTotalWithoutReasoning < 0
            TotalTokens = Math.Max(current.TotalTokens - previousTotalWithoutReasoning, 0), // in case the model gives us crappy results and current.TotalTokens - previousTotalWithoutReasoning < 0
            CachedTokens = current.CachedTokens, // we don't want to subtract cached tokens, as they are only for the last response
            CompletionTokens = current.CompletionTokens, // we don't want to subtract completion tokens, as they are only for the last response
            ReasoningTokens = current.ReasoningTokens
        };
    }

    internal void Write(AsyncBlittableJsonTextWriter writer)
    {
        writer.WriteStartObject();

        writer.WritePropertyName(nameof(PromptTokens));
        writer.WriteInteger(PromptTokens);
        writer.WriteComma();
        
        writer.WritePropertyName(nameof(CompletionTokens));
        writer.WriteInteger(CompletionTokens);
        writer.WriteComma();
        
        writer.WritePropertyName(nameof(TotalTokens));
        writer.WriteInteger(TotalTokens);
        writer.WriteComma();
        
        writer.WritePropertyName(nameof(CachedTokens));
        writer.WriteInteger(CachedTokens);
        writer.WriteComma();

        writer.WritePropertyName(nameof(ReasoningTokens));
        writer.WriteInteger(ReasoningTokens);
        writer.WriteEndObject();
    }

}
