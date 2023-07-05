using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Corax;
using Corax.Pipeline;
using Corax.Utils;
using Lucene.Net.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal struct CoraxDocumentTrainEnumerator : IReadOnlySpanEnumerator
{
    private readonly DocumentsStorage _documentStorage;
    private readonly QueryOperationContext _queryContext;
    private readonly TransactionOperationContext _indexContext;
    private readonly Index _index;
    private readonly IndexType _indexType;
    private readonly CoraxDocumentConverterBase _converter;
    private readonly HashSet<string> _collections;
    private readonly int _take;
    private IEnumerator<AnalyzedToken> _itemsEnumerable;

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
    }

    private struct AnalyzedToken
    {
        public byte[] Storage;
        public int Start;
        public uint Length;
    }


    private bool CanAcceptObject(object value)
    {
        if (value is LazyStringValue lsv && lsv.Size > 3)
            return true;

        if (value is LazyCompressedStringValue lcsv && lcsv.UncompressedSize > 3)
            return true;

        if (value is string sv && sv.Length > 3)
            return true;

        if (value is Enum)
            return true;

        if (value is bool)
            return true;

        if (value is DateTime)
            return true;

        if (value is DateTimeOffset)
            return true;

        if (value is TimeSpan)
            return true;

        if (value is LazyNumberValue || value is double || value is decimal || value is float)
            return true;

        if (value is DateOnly)
            return true;

        if (value is TimeOnly)
            return true;

        return false;
    }

    private IEnumerable<AnalyzedToken> GetItems()
    {
        var lowercaseAnalyzer = Analyzer.CreateLowercaseAnalyzer(_indexContext.Allocator);
        var scope = new IndexingStatsScope(new IndexingRunStats());
        foreach (var collection in _collections)
        {
            using var itemEnumerator = _index.GetMapEnumerator(GetItemsEnumerator(_queryContext, collection, _take), collection, _indexContext, scope, _indexType);

            var wordsBuffer = new byte[1024];
            var tokenBuffer = new Token[1024];

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
                    if (CanAcceptObject(result) == false)
                        continue;

                    using var __ = _converter.SetDocument(doc.LowerId, null, result, _indexContext, out var id, out var output, out _, out _);

                    var reader = new IndexEntryReader(output);
                    for (int i = 0; i < fields.Count; i++)
                    {
                        var field = fields.GetByFieldId(i);
                        var analyzer = field.Analyzer ?? lowercaseAnalyzer;

                        if (reader.GetFieldReaderFor(field.FieldId).Read(out _, out var value) == false)
                            continue;

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
                            analyzer.Execute(value, ref wordsSpan, ref tokenSpan);
                            items = tokenSpan.Length;
                        }

                        for (int j = 0; j < items; j++)
                        {
                            yield return new AnalyzedToken
                            {
                                Storage = wordsBuffer, 
                                Length = tokenBuffer[j].Length, 
                                Start = tokenBuffer[j].Offset
                            };
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
        output = current.Storage.AsSpan(current.Start, (int)current.Length);
        return true;
    }
}
