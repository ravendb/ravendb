using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public class EmbeddingPathConfiguration : IDynamicJson
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

    internal static bool Compare(EmbeddingPathConfiguration left, EmbeddingPathConfiguration right)
    {
        if (left == null && right == null)
            return true;
        
        if (left == null || right == null)
            return false;
        
        return left.Path == right.Path && 
               ChunkingOptions.Compare(left.ChunkingOptions, right.ChunkingOptions);
    }
}
