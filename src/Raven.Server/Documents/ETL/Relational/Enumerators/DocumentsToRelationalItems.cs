using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Relational.Enumerators;

public sealed class DocumentsToRelationalItems : IEnumerator<ToRelationalItem>
{
    private readonly IEnumerator<Document> _docs;
    private readonly string _collection;

    internal DocumentsToRelationalItems(IEnumerator<Document> docs, string collection)
    {
        _docs = docs;
        _collection = collection;
    }

    public bool MoveNext()
    {
        if (_docs.MoveNext() == false)
            return false;

        Current = new ToRelationalItem(_docs.Current, _collection);

        return true;
    }

    public void Reset()
    {
        throw new System.NotImplementedException();
    }

    object IEnumerator.Current => Current;

    public void Dispose()
    {
    }

    public ToRelationalItem Current { get; private set; }
}

