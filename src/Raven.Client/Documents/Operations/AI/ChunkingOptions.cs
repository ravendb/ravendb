using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public class ChunkingOptions : IDynamicJson
{
    public ChunkingMethod ChunkingMethod { get; set; }

    public int MaxTokensPerChunk { get; set; } = 512;

    public int OverlapTokens { get; set; } = 0;

    internal static readonly HashSet<ChunkingMethod> MethodsSupportingOverlapTokens = [ChunkingMethod.MarkDownSplitParagraphs, ChunkingMethod.PlainTextSplitParagraphs];

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ChunkingMethod)] = ChunkingMethod, 
            [nameof(MaxTokensPerChunk)] = MaxTokensPerChunk,
            [nameof(OverlapTokens)] = OverlapTokens
        };
    }

    internal void Validate(string source, List<string> errors)
    {
        if (MaxTokensPerChunk <= 0)
            errors.Add($"'{source}': {nameof(MaxTokensPerChunk)} value has to be greater than 0.");
        
        if (OverlapTokens < 0)
            errors.Add($"'{source}': {nameof(OverlapTokens)} value cannot be negative.");
        
        if (OverlapTokens > MaxTokensPerChunk)
            errors.Add($"'{source}': {nameof(OverlapTokens)} cannot be greater than {nameof(MaxTokensPerChunk)}.");
        
        if (OverlapTokens > 0 &&
            MethodsSupportingOverlapTokens.Contains(ChunkingMethod) == false)
            errors.Add($"'{source}': {nameof(OverlapTokens)} option is only supported for the following chunking methods: {string.Join(", ", MethodsSupportingOverlapTokens)}.");
    }

    internal static bool Compare(ChunkingOptions left, ChunkingOptions right)
    {
        if (left == null && right == null)
            return true;
        
        if (left == null || right == null)
            return false;
        
        return left.ChunkingMethod == right.ChunkingMethod &&
               left.MaxTokensPerChunk == right.MaxTokensPerChunk &&
               left.OverlapTokens == right.OverlapTokens;
    }
}

public enum ChunkingMethod
{
    PlainTextSplit,
    PlainTextSplitLines,
    PlainTextSplitParagraphs,
    MarkDownSplitLines,
    MarkDownSplitParagraphs,
    HtmlStrip
}
