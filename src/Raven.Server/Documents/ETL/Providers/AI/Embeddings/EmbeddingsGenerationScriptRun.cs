using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings;

public class EmbeddingsGenerationScriptRun : IEnumerable<EmbeddingGenerationScriptResult>
{
    public List<EmbeddingGenerationScriptResult> Additions { get; set; }
    public List<EmbeddingGenerationScriptResult> Removals { get; set; }

    public EmbeddingsGenerationScriptRun()
    {
        Additions = new List<EmbeddingGenerationScriptResult>();
        Removals = new List<EmbeddingGenerationScriptResult>();
    }

    public IEnumerator<EmbeddingGenerationScriptResult> GetEnumerator()
    {
        throw new System.NotImplementedException();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        throw new System.NotImplementedException();
    }
}
