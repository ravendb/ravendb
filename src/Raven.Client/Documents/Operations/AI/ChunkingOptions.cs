using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Options controlling how input text is chunked before generating or querying embeddings.
/// </summary>
public class ChunkingOptions : IDynamicJsonValueConvertible
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
    /// Serializes the chunking options to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ChunkingMethod)] = ChunkingMethod, 
            [nameof(MaxTokensPerChunk)] = MaxTokensPerChunk
        };
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
