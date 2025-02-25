namespace Raven.Client.Documents.Operations.AI;

public class ChunkingOptions
{
    public ChunkingMethod ChunkingMethod { get; set; }
    // todo set reasonable default
    public int MaxTokensPerChunk { get; set; } = 5;
}

public enum ChunkingMethod
{
    PlainTextSplitLines = 0,
    PlainTextSplitParagraphs = 1,
    MarkDownSplitLines = 2,
    MarkDownSplitParagraphs = 3,
    HtmlSplitLines = 4,
    HtmlStrip = 5
}
