using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticMapIndex : MapIndexBase<StaticMapIndexDefinition>
    {
        private readonly HashSet<string> _referencedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        protected internal readonly StaticIndexBase _compiled;

        private HandleReferences _handleReferences;

        private int _actualMaxNumberOfIndexOutputs;

        private int _maxNumberOfIndexOutputs;

        private StaticMapIndex(int indexId, StaticMapIndexDefinition definition, StaticIndexBase compiled)
            : base(indexId, IndexType.Map, definition)
        {
            _compiled = compiled;

            if (_compiled.ReferencedCollections == null)
                return;

            foreach (var collection in _compiled.ReferencedCollections)
            {
                foreach (var referencedCollection in collection.Value)
                    _referencedCollections.Add(referencedCollection.Name);
            }
        }

        public override bool HasBoostedFields => _compiled.HasBoostedFields;

        protected override void InitializeInternal()
        {
            _maxNumberOfIndexOutputs = Definition.IndexDefinition.Configuration.MaxIndexOutputsPerDocument ?? Configuration.MaxMapIndexOutputsPerDocument;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>();
            workers.Add(new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, null));

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleReferences(this, _compiled.ReferencedCollections, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            workers.Add(new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, null, Configuration));

            return workers.ToArray();
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            if (_referencedCollections.Count > 0)
                _handleReferences.HandleDelete(tombstone, collection, writer, indexContext, stats);

            base.HandleDelete(tombstone, collection, writer, indexContext, stats);
        }

        protected override bool IsStale(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff = null)
        {
            var isStale = base.IsStale(databaseContext, indexContext, cutoff);
            if (isStale || _referencedCollections.Count == 0)
                return isStale;

            foreach (var collection in Collections)
            {
                HashSet<CollectionName> referencedCollections;
                if (_compiled.ReferencedCollections.TryGetValue(collection, out referencedCollections) == false)
                    continue;

                foreach (var referencedCollection in referencedCollections)
                {
                    var lastDocEtag = DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(databaseContext, referencedCollection.Name);
                    var lastProcessedReferenceEtag = _indexStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection);

                    if (cutoff == null)
                    {
                        if (lastDocEtag > lastProcessedReferenceEtag)
                            return true;

                        var lastTombstoneEtag = DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(databaseContext, referencedCollection.Name);
                        var lastProcessedTombstoneEtag = _indexStorage.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection);

                        if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                            return true;
                    }
                    else
                    {
                        if (Math.Min(cutoff.Value, lastDocEtag) > lastProcessedReferenceEtag)
                            return true;

                        if (DocumentDatabase.DocumentsStorage.GetNumberOfTombstonesWithDocumentEtagLowerThan(databaseContext, referencedCollection.Name, cutoff.Value) > 0)
                            return true;
                    }
                }
            }

            return false;
        }

        protected override void HandleDocumentChange(DocumentChangeNotification notification)
        {
            if (HandleAllDocs == false && Collections.Contains(notification.CollectionName) == false && _referencedCollections.Contains(notification.CollectionName) == false)
                return;

            _mre.Set();
        }

        protected override unsafe long CalculateIndexEtag(bool isStale, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            if (_referencedCollections.Count == 0)
                return base.CalculateIndexEtag(isStale, documentsContext, indexContext);

            var minLength = MinimumSizeForCalculateIndexEtagLength();
            var length = minLength +
                         sizeof(long) * 2 * (Collections.Count * _referencedCollections.Count); // last referenced collection etags and last processed reference collection etags

            var indexEtagBytes = stackalloc byte[length];

            CalculateIndexEtagInternal(indexEtagBytes, isStale, documentsContext, indexContext);

            var writePos = indexEtagBytes + minLength;

            foreach (var collection in Collections)
            {
                HashSet<CollectionName> referencedCollections;
                if (_compiled.ReferencedCollections.TryGetValue(collection, out referencedCollections) == false)
                    continue;

                foreach (var referencedCollection in referencedCollections)
                {
                    var lastDocEtag = DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(documentsContext, referencedCollection.Name);
                    var lastMappedEtag = _indexStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection);

                    *(long*)writePos = lastDocEtag;
                    writePos += sizeof(long);
                    *(long*)writePos = lastMappedEtag;
                }
            }

            unchecked
            {
                return (long)Hashing.XXHash64.Calculate(indexEtagBytes, (ulong)length);
            }
        }

        public override int? ActualMaxNumberOfIndexOutputs
        {
            get
            {
                if (_actualMaxNumberOfIndexOutputs <= 1)
                    return null;

                return _actualMaxNumberOfIndexOutputs;
            }
        }

        public override int MaxNumberOfIndexOutputs => _maxNumberOfIndexOutputs;
        protected override bool EnsureValidNumberOfOutputsForDocument(int numberOfAlreadyProducedOutputs)
        {
            if (base.EnsureValidNumberOfOutputsForDocument(numberOfAlreadyProducedOutputs) == false)
                return false;

            if (Definition.IndexDefinition.Configuration.MaxIndexOutputsPerDocument.HasValue)
            {
                // user has specifically configured this value, but we don't trust it.

                if (_actualMaxNumberOfIndexOutputs < numberOfAlreadyProducedOutputs)
                    _actualMaxNumberOfIndexOutputs = numberOfAlreadyProducedOutputs;
            }

            return true;
        }

        public override Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return _compiled.ReferencedCollections;
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            return new StaticIndexDocsEnumerator(documents, _compiled.Maps[collection], collection, stats, StaticIndexDocsEnumerator.EnumerationType.Index);
        }

        public override Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            {
                using (var tx = context.OpenReadTransaction())
                {
                    var etags = GetLastProcessedDocumentTombstonesPerCollection(tx);

                    if (_referencedCollections.Count <= 0)
                        return etags;

                    foreach (var collection in Collections)
                    {
                        HashSet<CollectionName> referencedCollections;
                        if (_compiled.ReferencedCollections.TryGetValue(collection, out referencedCollections) == false)
                            throw new InvalidOperationException("Should not happen ever!");

                        foreach (var referencedCollection in referencedCollections)
                        {
                            var etag = _indexStorage.ReadLastProcessedReferenceTombstoneEtag(tx, collection, referencedCollection);
                            long currentEtag;
                            if (etags.TryGetValue(referencedCollection.Name, out currentEtag) == false || etag < currentEtag)
                                etags[referencedCollection.Name] = etag;
                        }
                    }

                    return etags;
                }
            }
        }

        public static Index CreateNew(int indexId, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = CreateIndexInstance(indexId, definition);
            instance.Initialize(documentDatabase, new IndexingConfigurationWithClientOverrides(definition.Configuration, documentDatabase.Configuration));

            return instance;
        }

        public static Index Open(int indexId, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var definition = StaticMapIndexDefinition.Load(environment);
            var instance = CreateIndexInstance(indexId, definition);

            instance.Initialize(environment, documentDatabase, new IndexingConfigurationWithClientOverrides(definition.Configuration, documentDatabase.Configuration));

            return instance;
        }

        private static StaticMapIndex CreateIndexInstance(int indexId, IndexDefinition definition)
        {
            var staticIndex = IndexAndTransformerCompilationCache.GetIndexInstance(definition);

            var staticMapIndexDefinition = new StaticMapIndexDefinition(definition, staticIndex.Maps.Keys.ToArray(), staticIndex.OutputFields, staticIndex.HasDynamicFields);
            var instance = new StaticMapIndex(indexId, staticMapIndexDefinition, staticIndex);
            return instance;
        }
    }
}