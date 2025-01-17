using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.AI.Enumerators;

public sealed class DocumentsToAiEtlItems : IEnumerator<AiEtlItem>
{
    private readonly IEnumerator<Document> _docs;
    private readonly string _collection;
    
    public AiEtlItem Current { get; private set; }
    object IEnumerator.Current => Current;
    
    public DocumentsToAiEtlItems(IEnumerator<Document> docs, string collection)
    {
        _docs = docs;
        _collection = collection;
    }
    
    public bool MoveNext()
    {
        if (_docs.MoveNext() == false)
            return false;

        Current = new AiEtlItem(_docs.Current, _collection);

        return true;
    }

    public void Reset()
    {
        throw new System.NotImplementedException();
    }

    public void Dispose()
    {
    }
}
