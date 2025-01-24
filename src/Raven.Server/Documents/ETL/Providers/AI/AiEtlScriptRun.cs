using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.AI;

public class AiEtlScriptRun : IEnumerable<EmbeddingRepresentation>
{
    public List<EmbeddingRepresentation> CurrentRun { get; set; }
    public Dictionary<string, Dictionary<string, List<string>>> Runs { get; set; }
    
    public AiEtlScriptRun()
    {
        CurrentRun = new List<EmbeddingRepresentation>();
        Runs = new Dictionary<string, Dictionary<string, List<string>>>();
    }
    
    public IEnumerator<EmbeddingRepresentation> GetEnumerator()
    {
        throw new System.NotImplementedException();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        throw new System.NotImplementedException();
    }
}
