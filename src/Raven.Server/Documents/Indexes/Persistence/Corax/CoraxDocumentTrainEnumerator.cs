using System;
using System.Collections.Generic;
using System.Threading;
using Corax;
using Corax.Pipeline;
using Corax.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal struct CoraxDocumentTrainEnumerator : IReadOnlySpanEnumerator
{
    private sealed class Builder : IndexWriter.IIndexEntryBuilder
    {
        private readonly ByteStringContext _allocator;
        private readonly List<(int FieldId, string FieldName, ByteString Value)> _terms;

        public Builder(ByteStringContext allocator,List<(int FieldId, string FieldName, ByteString)> terms)
        {
            _allocator = allocator;
            _terms = terms;
        }

        public void Boost(float boost)
        {
            
        }

        public ReadOnlySpan<byte> AnalyzeSingleTerm(int fieldId, ReadOnlySpan<byte> value)
        {
            return value; // not applicable 
        }

        public void WriteNull(int fieldId, string path)
        {
            _allocator.From(global::Corax.Constants.NullValueSlice.AsSpan(), out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, ReadOnlySpan<byte> value)
        {
            if (value.Length == 0)
                return;
            _allocator.From(value, out var b);
            _terms.Add((fieldId, null, b));
        }

        public void Write(int fieldId, string path, ReadOnlySpan<byte> value)
        {
            if (value.Length == 0)
                return;
            _allocator.From(value, out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, string path, string value)
        {
            if (value.Length == 0)
                return;
            _allocator.From(value, out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, ReadOnlySpan<byte> value, long longValue, double dblValue)
        {
            if (value.Length == 0)
                return;
            _allocator.From(value, out var b);
            _terms.Add((fieldId, null, b));
        }

        public void Write(int fieldId, string path, string value, long longValue, double dblValue)
        {
            if (value.Length == 0)
                return;
            _allocator.From(value, out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, string path, ReadOnlySpan<byte> value, long longValue, double dblValue)
        {
            if (value.Length == 0)
                return;
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

        public void RegisterEmptyOrNull(int fieldId, string fieldName, StoredFieldType type)
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

        public int ResetList()
        {
            return default;
        }

        public void RestoreList(int old)
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
    private readonly CancellationToken _token;
    
    public CoraxDocumentTrainEnumerator(TransactionOperationContext indexContext, CoraxDocumentConverterBase converter, Index index, IndexType indexType, DocumentsStorage storage, QueryOperationContext queryContext, HashSet<string> collections, CancellationToken token, int take = int.MaxValue)
    {
        _indexContext = indexContext;
        _index = index;
        _indexType = indexType;
        _converter = converter;
        _take = take;
        _token = token;

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
            using var itemEnumerator = _index.GetMapEnumerator(GetItemsEnumerator(_queryContext, collection, _take, _token), collection, _indexContext, scope, _indexType);
            while (true)
            {
                if (itemEnumerator.MoveNext(_queryContext.Documents, out var mapResults, out var _) == false)
                    break;

                var doc = (Document)itemEnumerator.Current.Item;

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
                            yield return new ArraySegment<byte>(wordsBuffer, tokenBuffer[j].Offset, (int)tokenBuffer[j].Length);
                        }
                    }
                }
            }
        }
    }

    private IEnumerator<Document> GetDocumentsEnumerator(QueryOperationContext queryContext, string collection, long take, CancellationToken token)
    {
        var size = queryContext.Documents.DocumentDatabase.Configuration.Databases.PulseReadTransactionLimit;
        var coraxDocumentTrainDocumentSource = new CoraxDocumentTrainSourceEnumerator(_documentStorage);
        
        if (collection == Constants.Documents.Collections.AllDocumentsCollection)
            return new PulsedTransactionEnumerator<Document, CoraxDocumentTrainSourceState>(queryContext.Documents,
                state => coraxDocumentTrainDocumentSource.GetUniformlyDistributedDocumentsFrom(queryContext.Documents, state), new(queryContext.Documents, size, take, token)); 

        return new PulsedTransactionEnumerator<Document,CoraxDocumentTrainSourceState>(queryContext.Documents, 
            state =>  coraxDocumentTrainDocumentSource.GetUniformlyDistributedDocumentsFrom(queryContext.Documents, collection, state)
            , new CoraxDocumentTrainSourceState(queryContext.Documents, size, take, token));
    }

    private IEnumerable<IndexItem> GetItemsEnumerator(QueryOperationContext queryContext, string collection, long take, CancellationToken token)
    {
        foreach (var document in GetDocumentsEnumerator(queryContext, collection, take, token))
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
