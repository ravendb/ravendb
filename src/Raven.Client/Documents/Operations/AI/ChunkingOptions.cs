using System;
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

    internal static bool AreEqual(ChunkingOptions left, ChunkingOptions right)
    {
        if (left == null && right == null)
            return true;
        
        if (left == null || right == null)
            return false;
        
        return left.Equals(right);
    }

    private bool Equals(ChunkingOptions other)
    {
        return ChunkingMethod == other.ChunkingMethod && 
               MaxTokensPerChunk == other.MaxTokensPerChunk && 
               OverlapTokens == other.OverlapTokens;
    }

    public override bool Equals(object obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;
        return Equals((ChunkingOptions)obj);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine((int)ChunkingMethod, MaxTokensPerChunk, OverlapTokens);
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
