using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public class AiUsage : IDynamicJsonValueConvertible
{
    public int PromptTokens { get; set; }
    public int CompletionTokens { get; set; }
    public int TotalTokens { get; set; }
    public int CachedTokens { get; set; }
    public int ReasoningTokens { get; set; }

    internal void UpdateFrom(BlittableJsonReaderObject json)
    {
        json.TryGet("prompt_tokens", out int promptTokens);
        json.TryGet("completion_tokens", out int completionTokens);
        json.TryGet("total_tokens", out int totalTokens);

        PromptTokens += promptTokens;
        CompletionTokens += completionTokens;
        TotalTokens += totalTokens;

        if (json.TryGet("prompt_tokens_details", out BlittableJsonReaderObject promptDetails) && promptDetails != null)
        {
            if (promptDetails.TryGet("cached_tokens", out int cachedTokens))
                CachedTokens += cachedTokens;
        }

        if (json.TryGet("completion_tokens_details", out BlittableJsonReaderObject completionTokensDetails) && completionTokensDetails != null)
        {
            if (completionTokensDetails.TryGet("reasoning_tokens", out int reasoningTokens))
            {
                ReasoningTokens += reasoningTokens;
            }
        }
    }

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
        writer.WriteEndObject();
    }

}
