using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Enumerators;

public sealed class DocumentsToRelationalDatabaseItems : IEnumerator<ToRelationalDatabaseItem>
{
    private readonly IEnumerator<Document> _docs;
    private readonly string _collection;

    internal DocumentsToRelationalDatabaseItems(IEnumerator<Document> docs, string collection)
    {
        _docs = docs;
        _collection = collection;
    }

    public bool MoveNext()
    {
        if (_docs.MoveNext() == false)
            return false;

        Current = new ToRelationalDatabaseItem(_docs.Current, _collection);

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

    public ToRelationalDatabaseItem Current { get; private set; }
}

