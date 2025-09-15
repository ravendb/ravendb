using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public class ChunkingOptions : IDynamicJsonValueConvertible
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

    public bool ValidateOverlapTokensProperty()
    {
        if (OverlapTokens < 0)
            return false;
        
        if (OverlapTokens > 0 &&
            MethodsSupportingOverlapTokens.Contains(ChunkingMethod) == false)
            return false;
        
        return true;
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
