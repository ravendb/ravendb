using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Indexes.Static
{
    public class CurrentIndexingScope : IDisposable
    {
        private IndexingStatsScope _stats;
        private IndexingStatsScope _loadDocumentStats;
        private readonly DocumentsStorage _documentsStorage;
        private readonly DocumentsOperationContext _documentsContext;

        public readonly UnmanagedBuffersPoolWithLowMemoryHandling UnmanagedBuffersPool;

        private readonly Func<string, SpatialField> _getSpatialField;

        /// [collection: [key: [referenceKeys]]]
        public Dictionary<string, Dictionary<string, HashSet<Slice>>> ReferencesByCollection;

        /// [collection: [collectionKey: etag]]
        public Dictionary<string, Dictionary<string, long>> ReferenceEtagsByCollection;

        [ThreadStatic]
        public static CurrentIndexingScope Current;

        static CurrentIndexingScope()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => Current = null;
        }

        public readonly Index Index;

        public DynamicBlittableJson Source;

        public string SourceCollection;

        public readonly TransactionOperationContext IndexContext;

        public readonly IndexDefinitionBase IndexDefinition;

        public LuceneDocumentConverter CreateFieldConverter;

        public CurrentIndexingScope(Index index, DocumentsStorage documentsStorage, DocumentsOperationContext documentsContext, IndexDefinitionBase indexDefinition, TransactionOperationContext indexContext, Func<string, SpatialField> getSpatialField, UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool)
        {
            _documentsStorage = documentsStorage;
            _documentsContext = documentsContext;
            Index = index;
            UnmanagedBuffersPool = _unmanagedBuffersPool;
            IndexDefinition = indexDefinition;
            IndexContext = indexContext;
            _getSpatialField = getSpatialField;
        }

        public void SetSourceCollection(string collection, IndexingStatsScope stats)
        {
            SourceCollection = collection;
            _stats = stats;
            _loadDocumentStats = null;
        }

        public unsafe dynamic LoadDocument(LazyStringValue keyLazy, string keyString, string collectionName)
        {
            using (_loadDocumentStats?.Start() ?? (_loadDocumentStats = _stats?.For(IndexingOperation.LoadDocument)))
            {
                if (keyLazy == null && keyString == null)
                    return DynamicNullObject.Null;

                var source = Source;
                if (source == null)
                    throw new ArgumentException("Cannot execute LoadDocument. Source is not set.");

                var id = source.GetId() as LazyStringValue;
                if (id == null)
                    throw new ArgumentException("Cannot execute LoadDocument. Source does not have a key.");

                if (keyLazy != null && id.Equals(keyLazy))
                    return source;

                if (keyString != null && id.Equals(keyString))
                    return source;

                Slice keySlice;
                if (keyLazy != null)
                {
                    if (keyLazy.Length == 0)
                        return DynamicNullObject.Null;

                    // we intentionally don't dispose of the scope here, this is being tracked by the references
                    // and will be disposed there.
                    Slice.External(_documentsContext.Allocator, keyLazy.Buffer, keyLazy.Size, out keySlice);
                }
                else
                {
                    if (keyString.Length == 0)
                        return DynamicNullObject.Null;
                    // we intentionally don't dispose of the scope here, this is being tracked by the references
                    // and will be disposed there.
                    Slice.From(_documentsContext.Allocator, keyString, out keySlice);
                }

                // making sure that we normalize the case of the key so we'll be able to find
                // it in case insensitive manner
                _documentsContext.Allocator.ToLowerCase(ref keySlice.Content);

                var references = GetReferencesForDocument(id);
                var referenceEtags = GetReferenceEtags();

                references.Add(keySlice);

                // when there is conflict, we need to apply same behavior as if the document would not exist
                var document = _documentsStorage.Get(_documentsContext, keySlice, throwOnConflict: false);

                if (document == null)
                {
                    MaybeUpdateReferenceEtags(referenceEtags, collectionName, 0);
                    return DynamicNullObject.Null;
                }

                MaybeUpdateReferenceEtags(referenceEtags, collectionName, document.Etag);

                // we can't share one DynamicBlittableJson instance among all documents because we can have multiple LoadDocuments in a single scope
                return new DynamicBlittableJson(document);
            }
        }

        public SpatialField GetOrCreateSpatialField(string name)
        {
            return _getSpatialField(name);
        }

        public void Dispose()
        {
            Current = null;
        }

        private static void MaybeUpdateReferenceEtags(Dictionary<string, long> referenceEtags, string collection, long etag)
        {
            if (referenceEtags.TryGetValue(collection, out long oldEtag) == false)
            {
                referenceEtags[collection] = etag;
                return;
            }

            if (oldEtag >= etag)
                return;

            referenceEtags[collection] = etag;
        }

        private Dictionary<string, long> GetReferenceEtags()
        {
            if (ReferenceEtagsByCollection == null)
                ReferenceEtagsByCollection = new Dictionary<string, Dictionary<string, long>>(StringComparer.OrdinalIgnoreCase);

            if (ReferenceEtagsByCollection.TryGetValue(SourceCollection, out Dictionary<string, long> referenceEtags) == false)
                ReferenceEtagsByCollection.Add(SourceCollection, referenceEtags = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase));

            return referenceEtags;
        }

        private HashSet<Slice> GetReferencesForDocument(string key)
        {
            if (ReferencesByCollection == null)
                ReferencesByCollection = new Dictionary<string, Dictionary<string, HashSet<Slice>>>(StringComparer.OrdinalIgnoreCase);

            if (ReferencesByCollection.TryGetValue(SourceCollection, out Dictionary<string, HashSet<Slice>> referencesByCollection) == false)
                ReferencesByCollection.Add(SourceCollection, referencesByCollection = new Dictionary<string, HashSet<Slice>>());

            if (referencesByCollection.TryGetValue(key, out HashSet<Slice> references) == false)
                referencesByCollection.Add(key, references = new HashSet<Slice>(SliceComparer.Instance));

            return references;
        }
    }
}
