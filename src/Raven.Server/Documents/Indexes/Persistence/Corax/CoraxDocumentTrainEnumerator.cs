using System;
using System.Collections.Generic;
using System.Text.Unicode;
using System.Threading;
using Corax.Analyzers;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal class CoraxDocumentTrainEnumerator : IReadOnlySpanEnumerator
{
    private sealed class Builder : IIndexEntryBuilder
    {
        private int _storageIdx;

        private readonly byte[] _storage;
        private readonly byte[] _words;
        private readonly Token[] _tokens;
        private readonly List<ArraySegment<byte>> _segments;
        private readonly Analyzer _lowercaseAnalyzer;
        private readonly IndexFieldsMapping _mapping;

        public List<ArraySegment<byte>> Buffer => _segments;

        public Builder(ByteStringContext allocator, IndexFieldsMapping mapping)
        {
            _lowercaseAnalyzer = Analyzer.CreateLowercaseAnalyzer(allocator);
            _mapping = mapping;

            // RavenDB-21043: We wont process anything bigger than 4K, the most likely case is that we are processing
            // a huge document which will cost us a huge amount of allocations and monopolize the dictionary.
            _storage = new byte[4096];
            _words = new byte[256];
            _tokens = new Token[256];
            _segments = new List<ArraySegment<byte>>();

            _storageIdx = 0;
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
            // nothing to do here
        }

        public void WriteNonExistingMarker(int fieldId, string path)
        {
            // nothing to do
        }

        public void Write(int fieldId, ReadOnlySpan<byte> value)
        {
            if (value.Length == 0)
                return;

            if (_mapping.TryGetByFieldId(fieldId, out var field) == false)
                return;

            // Ensure that we will have enough space to write the content plus an space. 
            int startLocation = 0;
            int maxSize = Math.Min(128, value.Length);

            if (value.Length > maxSize)
            {
                // The value is too big, therefore we will just select 128 bytes out from a random place.
                startLocation = Random.Shared.Next(value.Length - maxSize);
            }

            ProcessSelectedStream(field.Analyzer ?? _lowercaseAnalyzer, value.Slice(startLocation, maxSize));
        }

        public void Write(int fieldId, string path, ReadOnlySpan<byte> value)
        {
           Write(fieldId, value);
        }

        public void Write(int fieldId, string path, string value)
        {
            if (value.Length == 0)
                return;

            if (_mapping.TryGetByFieldId(fieldId, out var field) == false)
                return;

            // Ensure that we will have enough space to write the content plus an space. 
            int startLocation = 0;
            int maxSize = Math.Min(128, value.Length);

            if (value.Length > maxSize)
            {
                // The value is too big, therefore we will just select 128 bytes out from a random place.
                startLocation = Random.Shared.Next(value.Length - maxSize);
            }

            // We select only the part we are gonna use, we transcode into UTF8 and then process the stream.
            Utf8.FromUtf16(value.AsSpan(startLocation, maxSize), _words, out _, out var bytesWritten);
            ProcessSelectedStream(field.Analyzer ?? _lowercaseAnalyzer, _words.AsSpan(0, bytesWritten));
        }

        private void ProcessSelectedStream(Analyzer analyzer, ReadOnlySpan<byte> value)
        {
            // Execute the analyzer.
            var wordsSpan = _words.AsSpan();
            var tokensSpan = _tokens.AsSpan();
            analyzer.Execute(value, ref wordsSpan, ref tokensSpan);

            // We copy from the value into the builder local storage.
            var storageSpan = _storage.AsSpan(_storageIdx);
            foreach (var token in tokensSpan)
            {
                // We wont process anything that is less than a 3-gram
                if (token.Length < 3)
                    continue;

                // We are done if there is no more space.
                if (storageSpan.Length < token.Length)
                    return;

                wordsSpan.Slice(token.Offset, (int)token.Length)
                    .CopyTo(_storage.AsSpan(_storageIdx, (int)token.Length));

                _segments.Add(new ArraySegment<byte>(_storage, _storageIdx, (int)token.Length));
                _storageIdx += (int)token.Length;
            }
        }

        public void Write(int fieldId, ReadOnlySpan<byte> value, long longValue, double dblValue)
        {
            Write(fieldId, value);
        }

        public void Write(int fieldId, string path, string value, long longValue, double dblValue)
        {
            Write(fieldId, path, value);
        }

        public void Write(int fieldId, string path, ReadOnlySpan<byte> value, long longValue, double dblValue)
        {
            Write(fieldId, value);
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

        public void Reset()
        {
            _storageIdx = 0;
            _segments.Clear();
        }
    }

    private readonly DocumentsStorage _documentStorage;
    private readonly DocumentsOperationContext _docsContext;
    private readonly TransactionOperationContext _indexContext;
    private readonly Index _index;
    private readonly IndexType _indexType;
    private readonly CoraxDocumentConverterBase _converter;
    private readonly HashSet<string> _collections;
    private readonly int _take;
    private IEnumerator<ArraySegment<byte>> _itemsEnumerable;
    private readonly CancellationToken _token;
    private readonly Size _maxAllocatedMemory;
    private readonly IndexingStatsScope _indexingStatsScope;

    public int Count { get; private set; }

    public CoraxDocumentTrainEnumerator(TransactionOperationContext indexContext, CoraxDocumentConverterBase converter, Index index, IndexType indexType, DocumentsStorage storage, DocumentsOperationContext docsContext, HashSet<string> collections, CancellationToken token, IndexingStatsScope indexingStatsScope, int take = int.MaxValue)
    {
        _indexContext = indexContext;
        _index = index;
        _indexType = indexType;
        _converter = converter;
        _take = take;
        _token = token;
        _indexingStatsScope = indexingStatsScope;

        // RavenDB-21043: Tracking the total memory allocated by the thread is also a way to limit the total resources allocated
        // to the training process. We are currently limiting the default to 2Gb and we haven't seen any deterioration in the 
        // compression using that limit. However, given there is no limitation in 64bits mode, we could increase it if we find
        // cases which are not covered.
        _maxAllocatedMemory = _index.Configuration.MaxAllocationsAtDictionaryTraining / 2;

        _documentStorage = storage;
        _docsContext = docsContext;
        _collections = collections;

        Count = 0;
    }

    private IEnumerable<ArraySegment<byte>> GetItems()
    {
        // RavenDB-21043: Track the total allocations that we will allow each collection to use. The idea is that multi-collection indexes
        // use this number to also ensure that all collections have the opportunity to give samples to the training process.
        var maxAllocatedMemoryPerCollection = _maxAllocatedMemory / _collections.Count;

        var builder = new Builder(_indexContext.Allocator, _converter.GetKnownFieldsForWriter());

        foreach (var collection in _collections)
        {
            // We retrieve the baseline memory in order to calculate the difference.
            var atStartAllocated = new Size(NativeMemory.CurrentThreadStats.TotalAllocated, SizeUnit.Bytes);

            using var itemEnumerator = _index.GetMapEnumerator(GetItemsEnumerator(_docsContext, collection, _take, _token), collection, _indexContext, _indexingStatsScope, _indexType);
            while (true)
            {
                if (itemEnumerator.MoveNext(_docsContext, out var mapResults, out _) == false)
                    break;

                _indexingStatsScope.RecordMapAttempt();
                
                var doc = (Document)itemEnumerator.Current.Item;

                var enumerator = mapResults.GetEnumerator();
                do
                {
                    try
                    {
                        // When the index throws an exception as in the case of RavenDB-21480 that we try to
                        // divide by zero, we have to disregard the document only. However, since an exception
                        // in an enumerator will stop the enumeration, we need to guard against that case. In 
                        // this case what we do is ignore the document that will be in error and let the document
                        // to fail during the indexing instead. 
                        // https://issues.hibernatingrhinos.com/issue/RavenDB-21480

                        if (enumerator.MoveNext() == false)
                            break;
                    }
                    catch
                    {
                        _indexingStatsScope.RecordMapError();
                        continue;
                    }

                    builder.Reset();
                    _converter.SetDocument(doc.LowerId, null, enumerator.Current, _indexContext, builder);
                    
                    _indexingStatsScope.RecordMapSuccess();

                    foreach (var item in builder.Buffer)
                        yield return item;
                } 
                while (true);

                _indexingStatsScope.RecordDocumentSize(doc.Data.Size);
                
                // Check if we have already hit the threshold allocations.
                var totalNativeAllocations = new Size(NativeMemory.CurrentThreadStats.TotalAllocated, SizeUnit.Bytes);
                var totalAllocated = totalNativeAllocations - atStartAllocated;
                
                _indexingStatsScope.SetAllocatedUnmanagedBytes(totalNativeAllocations.GetValue(SizeUnit.Bytes));
                
                if (totalAllocated > maxAllocatedMemoryPerCollection)
                    break;
            }
        }
    }

    private IEnumerator<Document> GetDocumentsEnumerator(DocumentsOperationContext docsContext, string collection, long take, CancellationToken token)
    {
        var size = docsContext.DocumentDatabase.Configuration.Databases.PulseReadTransactionLimit;
        var coraxDocumentTrainDocumentSource = new CoraxDocumentTrainSourceEnumerator(_documentStorage);
        
        if (collection == Constants.Documents.Collections.AllDocumentsCollection)
            return new TransactionForgetAboutDocumentEnumerator(new PulsedTransactionEnumerator<Document, CoraxDocumentTrainSourceState>(docsContext,
                state => coraxDocumentTrainDocumentSource.GetDocumentsForDictionaryTraining(docsContext, state), new(docsContext, size, take, token)), docsContext); 

        return new TransactionForgetAboutDocumentEnumerator(new PulsedTransactionEnumerator<Document,CoraxDocumentTrainSourceState>(docsContext, 
            state =>  coraxDocumentTrainDocumentSource.GetDocumentsForDictionaryTraining(docsContext, collection, state)
            , new CoraxDocumentTrainSourceState(docsContext, size, take, token)), docsContext);
    }

    private IEnumerable<IndexItem> GetItemsEnumerator(DocumentsOperationContext docsContext, string collection, long take, CancellationToken token)
    {
        foreach (var document in GetDocumentsEnumerator(docsContext, collection, take, token))
        {
            yield return new DocumentIndexItem(document.Id, document.LowerId, document.Etag, document.LastModified, document.Data.Size, document, document.Flags);
        }
    }

    public void Reset()
    {
        Count = 0;
        _itemsEnumerable = GetItems().GetEnumerator();
    }

    public bool MoveNext(out ReadOnlySpan<byte> output)
    {
        _itemsEnumerable ??= GetItems().GetEnumerator();

        // RavenDB-21106: Since the training of dictionaries may cause us to trigger (critical) errors prematurely as without training
        // they would trigger during indexing and we don't want to replicate all the handling necessary for it. We will just ignore any
        // document where an error may happen during indexing, since it will also happen there and handled appropriately. 
        bool result;
        while (true)
        {
            try
            {
                result = _itemsEnumerable.MoveNext();
                break;
            }
            catch 
            {
                // Since there was an error, we will ignore this document and try again.
                _indexingStatsScope.RecordMapError();
            }
        }

        if (result == false)
        {
            output = ReadOnlySpan<byte>.Empty;
            return false;
        }

        var current = _itemsEnumerable.Current;
        output = current.AsSpan();
        Count++;
        return true;
    }
}
