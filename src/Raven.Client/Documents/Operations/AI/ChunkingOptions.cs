using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Options controlling how input text is chunked before generating or querying embeddings.
/// </summary>
public class ChunkingOptions : IDynamicJson
{
    /// <summary>
    /// The algorithm used to split the text into chunks.
    /// </summary>
    public ChunkingMethod ChunkingMethod { get; set; }

    /// <summary>
    /// The maximum number of tokens per chunk. Must be a positive number.
    /// </summary>
    public int MaxTokensPerChunk { get; set; } = 512;

    /// <summary>
    /// The number of tokens to overlap between consecutive chunks.
    /// </summary>
    public int OverlapTokens { get; set; } = 0;

    internal static readonly HashSet<ChunkingMethod> MethodsSupportingOverlapTokens = [ChunkingMethod.MarkDownSplitParagraphs, ChunkingMethod.PlainTextSplitParagraphs];

    /// <summary>
    /// Serializes the chunking options to a JSON structure.
    /// </summary>
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

/// <summary>
/// Supported methods for splitting text into chunks.
/// </summary>
public enum ChunkingMethod
{
    /// <summary>
    /// Split plain text by a generic strategy (implementation-defined), aiming for the specified token size.
    /// </summary>
    PlainTextSplit,
    /// <summary>
    /// Split plain text by line breaks.
    /// </summary>
    PlainTextSplitLines,
    /// <summary>
    /// Split plain text by paragraph separators (e.g., double line breaks).
    /// </summary>
    PlainTextSplitParagraphs,
    /// <summary>
    /// Split Markdown text by line breaks.
    /// </summary>
    MarkDownSplitLines,
    /// <summary>
    /// Split Markdown text by paragraph-level boundaries.
    /// </summary>
    MarkDownSplitParagraphs,
    /// <summary>
    /// Strip HTML markup and split resulting text using a sensible strategy.
    /// </summary>
    HtmlStrip
}
