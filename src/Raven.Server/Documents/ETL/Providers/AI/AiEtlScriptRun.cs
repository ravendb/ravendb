using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.AI;

public class AiEtlScriptRun : IEnumerable<AiEtlEmbeddingItem>
{
    public List<AiEtlEmbeddingItem> CurrentRun { get; set; }
    public Dictionary<string, Dictionary<string, List<string>>> Runs { get; set; }
    
    public AiEtlScriptRun()
    {
        CurrentRun = new List<AiEtlEmbeddingItem>();
        Runs = new Dictionary<string, Dictionary<string, List<string>>>();
    }
    
    public IEnumerator<AiEtlEmbeddingItem> GetEnumerator()
    {
        throw new System.NotImplementedException();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        throw new System.NotImplementedException();
    }
}
