using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public class EmbeddingPathConfiguration : IDynamicJsonValueConvertible
{
    public string Path { get; set; }
    
    public ChunkingOptions ChunkingOptions { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        var jsv = new DynamicJsonValue();
        jsv[nameof(Path)] = Path;
        jsv[nameof(ChunkingOptions)] = ChunkingOptions?.ToJson();
        return jsv;
    }
}
