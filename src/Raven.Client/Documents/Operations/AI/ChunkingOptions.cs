namespace Raven.Client.Documents.Operations.AI;

public class ChunkingOptions
{
    public ChunkingMethod ChunkingMethod { get; set; }
    public int MaxTokensPerChunk { get; set; }
}

public enum ChunkingMethod
{
    PlainTextSplitLines,
    PlainTextSplitParagraphs,
    MarkDownSplitLines,
    MarkDownSplitParagraphs,
    HtmlSplitLines,
    HtmlStrip
}
