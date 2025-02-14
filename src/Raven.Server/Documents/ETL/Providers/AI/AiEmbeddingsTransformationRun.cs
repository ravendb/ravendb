using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.AI;

public class AiEmbeddingsTransformationRun : IEnumerable<AiIntegrationEmbeddingItem>
{
    public List<AiIntegrationEmbeddingItem> Additions { get; set; }
    public List<AiIntegrationEmbeddingItem> Removals { get; set; }
    
    public AiEmbeddingsTransformationRun()
    {
        Additions = new List<AiIntegrationEmbeddingItem>();
        Removals = new List<AiIntegrationEmbeddingItem>();
    }
    
    public IEnumerator<AiIntegrationEmbeddingItem> GetEnumerator()
    {
        throw new System.NotImplementedException();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        throw new System.NotImplementedException();
    }
}
