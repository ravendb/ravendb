using System;
using System.Collections.Generic;
using Corax;
using Corax.Pipeline;
using Corax.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal struct CoraxDocumentTrainEnumerator : IReadOnlySpanEnumerator
{
    private class Builder : IndexWriter.IIndexEntryBuilder
    {
        private readonly ByteStringContext _allocator;
        private readonly List<(int FieldId, string FieldName, ByteString Value)> _terms;

        public Builder(ByteStringContext allocator,List<(int FieldId, string FieldName, ByteString)> terms)
        {
            _allocator = allocator;
            _terms = terms;
        }

        public bool IsEmpty => true;

        public void Boost(float boost)
        {
            
        }

        public void WriteNull(int fieldId, string path)
        {
            _allocator.From(global::Corax.Constants.NullValueSlice.AsSpan(), out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, ReadOnlySpan<byte> value)
        {
            _allocator.From(value, out var b);
            _terms.Add((fieldId, null, b));
        }

        public void Write(int fieldId, string path, ReadOnlySpan<byte> value)
        {
            _allocator.From(value, out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, string path, string value)
        {
            _allocator.From(value, out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, ReadOnlySpan<byte> value, long longValue, double dblValue)
        {
            _allocator.From(value, out var b);
            _terms.Add((fieldId, null, b));
        }

        public void Write(int fieldId, string path, string value, long longValue, double dblValue)
        {
            _allocator.From(value, out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, string path, ReadOnlySpan<byte> value, long longValue, double dblValue)
        {
            _allocator.From(value, out var b);
            _terms.Add((fieldId, path, b));
        }

        public void WriteSpatial(int fieldId, string path, CoraxSpatialPointEntry entry)
        {
            // nothing to do here
        }

        public void Store(BlittableJsonReaderObject storedValue)
        {
            // nothing to do
        }

        public void Store(int fieldId, string name, BlittableJsonReaderObject storedValue)
        {
            // nothing to do
        }

        public void IncrementList()
        {
            
        }

        public void DecrementList()
        {
        }
    }

    private readonly DocumentsStorage _documentStorage;
    private readonly QueryOperationContext _queryContext;
    private readonly TransactionOperationContext _indexContext;
    private readonly Index _index;
    private readonly IndexType _indexType;
    private readonly CoraxDocumentConverterBase _converter;
    private readonly HashSet<string> _collections;
    private readonly int _take;
    private IEnumerator<ArraySegment<byte>> _itemsEnumerable;
    private readonly List<(int FieldId, string FieldName, ByteString Value)> _terms;
    private readonly Builder _builder;

    public CoraxDocumentTrainEnumerator(TransactionOperationContext indexContext, CoraxDocumentConverterBase converter, Index index, IndexType indexType, DocumentsStorage storage, QueryOperationContext queryContext, HashSet<string> collections, int take = int.MaxValue)
    {
        _indexContext = indexContext;
        _index = index;
        _indexType = indexType;
        _converter = converter;
        _take = take;

        _documentStorage = storage;
        _queryContext = queryContext;
        _collections = collections;
        _terms = new List<(int FieldId, string FieldName, ByteString Value)>();
        _builder = new Builder(indexContext.Allocator, _terms);
    }

    private IEnumerable<ArraySegment<byte>> GetItems()
    {
        var lowercaseAnalyzer = Analyzer.CreateLowercaseAnalyzer(_indexContext.Allocator);
        var scope = new IndexingStatsScope(new IndexingRunStats());
        
        var wordsBuffer = new byte[1024];
        var tokenBuffer = new Token[1024];
        
        foreach (var collection in _collections)
        {
            using var itemEnumerator = _index.GetMapEnumerator(GetItemsEnumerator(_queryContext, collection, _take), collection, _indexContext, scope, _indexType);
            while (true)
            {
                if (itemEnumerator.MoveNext(_queryContext.Documents, out var mapResults, out var _) == false)
                    break;

                var doc = itemEnumerator.Current.Item as Document;
                if (doc == null)
                    continue;

                var fields = _converter.GetKnownFieldsForWriter();

                foreach (var result in mapResults)
                {
                    _terms.Clear();
                    _converter.SetDocument(doc.LowerId, null, result, _indexContext,_builder);
                    
                    for (int i = 0; i < _terms.Count; i++)
                    {
                        var (fieldId, fieldName, value) = _terms[i];

                        if (fields.TryGetByFieldId(fieldId, out var field) == false &&
                            fields.TryGetByFieldName(fieldName, out field) == false)
                            continue;
                        
                        var analyzer = field.Analyzer ?? lowercaseAnalyzer;

                        
                        if (value.Length < 3)
                            continue;
                    
                        if (value.Length > wordsBuffer.Length)
                        {
                            wordsBuffer = new byte[value.Length * 2];
                            tokenBuffer = new Token[value.Length * 2];
                        }
                    
                        int items;
                        {
                            var wordsSpan = wordsBuffer.AsSpan();
                            var tokenSpan = tokenBuffer.AsSpan();
                            analyzer.Execute(value.ToSpan(), ref wordsSpan, ref tokenSpan);
                            items = tokenSpan.Length;
                        }
                    
                        for (int j = 0; j < items; j++)
                        {
                            yield return new ArraySegment<byte>(wordsBuffer, tokenBuffer[i].Offset, (int)tokenBuffer[i].Length);
                        }
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
        if (result == false)
        {
            output = ReadOnlySpan<byte>.Empty;
            return false;
        }

        var current = _itemsEnumerable.Current;
        output = current.AsSpan();
        return true;
    }
}
