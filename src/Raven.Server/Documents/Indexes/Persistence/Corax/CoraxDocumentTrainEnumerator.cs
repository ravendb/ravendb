using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal struct CoraxDocumentTrainEnumerator : IReadOnlySpanEnumerator
{
    private readonly DocumentsStorage _documentStorage;
    private readonly QueryOperationContext _queryContext;
    private readonly TransactionOperationContext _indexContext;
    private readonly Index _index;
    private readonly IndexType _indexType;
    private readonly HashSet<string> _collections;
    private readonly int _take;
    private IEnumerator<LazyStringValue> _itemsEnumerable;

    public CoraxDocumentTrainEnumerator(TransactionOperationContext indexContext, Index index, IndexType indexType, DocumentsStorage storage, QueryOperationContext queryContext, HashSet<string> collections, int take = int.MaxValue)
    {
        _indexContext = indexContext;
        _index = index;
        _indexType = indexType;
        _take = take;

        _documentStorage = storage;
        _queryContext = queryContext;
        _collections = collections;
    }

    private IEnumerable<LazyStringValue> GetItems()
    {
        var scope = new IndexingStatsScope(new IndexingRunStats());
        foreach (var collection in _collections)
        {
            using var itemEnumerator = _index.GetMapEnumerator(GetItemsEnumerator(_queryContext, collection, _take), collection, _indexContext, scope, _indexType);
            while (true)
            {
                if (itemEnumerator.MoveNext(_queryContext.Documents, out var _, out var _) == false)
                    break;

                Document doc = itemEnumerator.Current.Item as Document;
                if (doc == null)
                    continue;

                var reader = doc.Data;
                int properties = reader.Count;
                BlittableJsonReaderObject.PropertyDetails details = default;
                for (int i = 0; i < properties; i++)
                {
                    reader.GetPropertyByIndex(i, ref details);
                    switch (details.Token)
                    {
                        case BlittableJsonToken.String:
                            yield return (LazyStringValue)details.Value;
                            break;
                        case BlittableJsonToken.CompressedString:
                            yield return ((LazyCompressedStringValue)details.Value).ToLazyStringValue();
                            break;
                    }
                }
            }
        }
    }

    private IEnumerable<Document> GetDocumentsEnumerator(QueryOperationContext queryContext, string collection, long take = int.MaxValue)
    {
        if (collection == Constants.Documents.Collections.AllDocumentsCollection)
            return _documentStorage.GetUniformlyDistributedDocumentsFrom(queryContext.Documents, take);
        return _documentStorage.GetUniformlyDistributedDocumentsFrom(queryContext.Documents, collection, take);
    }

    private IEnumerable<IndexItem> GetItemsEnumerator(QueryOperationContext queryContext, string collection, long take = int.MaxValue)
    {
        foreach (var document in GetDocumentsEnumerator(queryContext, collection, take))
        {
            yield return new DocumentIndexItem(document.Id, document.LowerId, document.Etag, document.LastModified, document.Data.Size, document);
        }
    }

    public void Reset()
    {
        _itemsEnumerable = GetItems().GetEnumerator();
    }

    public bool MoveNext(out ReadOnlySpan<byte> output)
    {
        _itemsEnumerable ??= GetItems().GetEnumerator();
        var result = _itemsEnumerable.MoveNext();
        output = result == true ? _itemsEnumerable.Current.AsSpan() : ReadOnlySpan<byte>.Empty;
        return result;
    }
}
