using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class HandleReferences : IIndexingWork
    {
        protected Logger _logger;

        private readonly StaticMapIndex _index;
        private readonly IndexingConfiguration _configuration;
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexStorage _indexStorage;

        private readonly Reference _reference = new Reference();

        public HandleReferences(StaticMapIndex index, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration)
        {
            _index = index;
            _configuration = configuration;
            _documentsStorage = documentsStorage;
            _indexStorage = indexStorage;
            _logger = LoggingSource.Instance
                .GetLogger<HandleReferences>(_indexStorage.DocumentDatabase.Name);
        }

        public string Name => "References";

        public bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            var pageSize = _configuration.MaxNumberOfDocumentsToFetchForMap;
            var timeoutProcessing = Debugger.IsAttached == false ? _configuration.DocumentProcessingTimeout.AsTimeSpan : TimeSpan.FromMinutes(15);

            var moreWorkFound = HandleDocuments(ActionType.Tombstone, databaseContext, indexContext, writeOperation, stats, pageSize, timeoutProcessing, token);
            moreWorkFound |= HandleDocuments(ActionType.Document, databaseContext, indexContext, writeOperation, stats, pageSize, timeoutProcessing, token);

            return moreWorkFound;
        }

        private bool HandleDocuments(ActionType actionType, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, int pageSize, TimeSpan timeoutProcessing, CancellationToken token)
        {
            var moreWorkFound = false;
            Dictionary<string, long> lastIndexedEtagsByCollection = null;

            foreach (var collection in _index.Collections)
            {
                HashSet<string> referencedCollections;
                if (_index._compiled.ReferencedCollections.TryGetValue(collection, out referencedCollections) == false)
                    continue;

                if (lastIndexedEtagsByCollection == null)
                    lastIndexedEtagsByCollection = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

                long lastIndexedEtag;
                if (lastIndexedEtagsByCollection.TryGetValue(collection, out lastIndexedEtag) == false)
                    lastIndexedEtagsByCollection[collection] = lastIndexedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                if (lastIndexedEtag == 0) // we haven't indexed yet, so we are skipping references for now
                    continue;

                foreach (var referencedCollection in referencedCollections)
                {
                    using (var collectionStats = stats.For("Collection_" + referencedCollection))
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Executing handle references for '{_index.Name} ({_index.IndexId})'. Collection: {referencedCollection}. Type: {actionType}.");

                        long lastReferenceEtag;

                        switch (actionType)
                        {
                            case ActionType.Document:
                                lastReferenceEtag = _indexStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection);
                                break;
                            case ActionType.Tombstone:
                                lastReferenceEtag = _indexStorage.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection);
                                break;
                            default:
                                throw new NotSupportedException();
                        }

                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Executing handle references for '{_index.Name} ({_index.IndexId})'. LastReferenceEtag: {lastReferenceEtag}.");

                        var lastEtag = lastReferenceEtag;
                        var count = 0;

                        var sw = Stopwatch.StartNew();
                        IndexWriteOperation indexWriter = null;

                        using (databaseContext.OpenReadTransaction())
                        {
                            IEnumerable<Reference> references;
                            switch (actionType)
                            {
                                case ActionType.Document:
                                    references = _documentsStorage
                                        .GetDocumentsAfter(databaseContext, referencedCollection, lastReferenceEtag + 1, 0, pageSize)
                                        .Select(document =>
                                        {
                                            _reference.Key = document.Key;
                                            _reference.Etag = document.Etag;

                                            return _reference;
                                        });
                                    break;
                                case ActionType.Tombstone:
                                    references = _documentsStorage
                                        .GetTombstonesAfter(databaseContext, referencedCollection, lastReferenceEtag , 0, pageSize)
                                        .Select(tombstone =>
                                        {
                                            _reference.Key = tombstone.Key;
                                            _reference.Etag = tombstone.Etag;

                                            return _reference;
                                        });
                                    break;
                                default:
                                    throw new NotSupportedException();
                            }

                            foreach (var referencedDocument in references)
                            {
                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Executing handle references for '{_index.Name} ({_index.IndexId})'. Processing reference: {referencedDocument.Key}.");

                                lastEtag = referencedDocument.Etag;
                                count++;

                                var documents = new List<Document>();
                                foreach (var key in _indexStorage
                                    .GetDocumentKeysFromCollectionThatReference(collection, referencedDocument.Key, indexContext.Transaction))
                                {
                                    var doc = _documentsStorage.Get(databaseContext, key);
                                    if(doc != null && doc.Etag <= lastIndexedEtag)
                                        documents.Add(doc);
                                }

                                using (var docsEnumerator = _index.GetMapEnumerator(documents, collection, indexContext))
                                {
                                    IEnumerable mapResults;

                                    while (docsEnumerator.MoveNext(out mapResults))
                                    {
                                        token.ThrowIfCancellationRequested();

                                        var current = docsEnumerator.Current;

                                        if (indexWriter == null)
                                            indexWriter = writeOperation.Value;

                                        if (_logger.IsInfoEnabled)
                                            _logger.Info($"Executing handle references for '{_index.Name} ({_index.IndexId})'. Processing document: {current.Key}.");

                                        try
                                        {
                                            _index.HandleMap(current.LoweredKey, mapResults, indexWriter, indexContext, collectionStats);
                                        }
                                        catch (Exception e)
                                        {
                                            if (_logger.IsInfoEnabled)
                                                _logger.Info($"Failed to execute mapping function on '{current.Key}' for '{_index.Name} ({_index.IndexId})'.", e);
                                        }

                                        if (sw.Elapsed > timeoutProcessing)
                                            break;
                                    }
                                }
                            }
                        }

                        if (count == 0)
                            continue;

                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Executing handle references for '{_index} ({_index.Name})'. Processed {count} references in '{referencedCollection}' collection in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                        switch (actionType)
                        {
                            case ActionType.Document:
                                _indexStorage.WriteLastReferenceEtag(indexContext.Transaction, collection, referencedCollection, lastEtag);
                                break;
                            case ActionType.Tombstone:
                                _indexStorage.WriteLastReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection, lastEtag);
                                break;
                            default:
                                throw new NotSupportedException();
                        }

                        moreWorkFound = true;
                    }
                }
            }

            return moreWorkFound;
        }

        public unsafe void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            var tombstoneKeySlice = Slice.External(indexContext.Transaction.InnerTransaction.Allocator, tombstone.LoweredKey.Buffer, tombstone.LoweredKey.Size);
            _indexStorage.RemoveReferences(tombstoneKeySlice, collection, null, indexContext.Transaction);
        }

        private enum ActionType
        {
            Document,
            Tombstone
        }

        private class Reference
        {
            public LazyStringValue Key;

            public long Etag;
        }
    }
}