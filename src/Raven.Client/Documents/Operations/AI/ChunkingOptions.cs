using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public class ChunkingOptions : IDynamicJsonValueConvertible
{
    public ChunkingMethod ChunkingMethod { get; set; }
    
    public int MaxTokensPerChunk { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        var djv = new DynamicJsonValue();
        djv[nameof(ChunkingMethod)] = ChunkingMethod;
        djv[nameof(MaxTokensPerChunk)] = MaxTokensPerChunk;
        return djv;
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
