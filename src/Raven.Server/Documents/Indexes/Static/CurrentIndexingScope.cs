using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Patch;
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
        private IndexingStatsScope _loadAttachmentStats;
        private IndexingStatsScope _loadCompareExchangeValueStats;
        private JavaScriptUtils _javaScriptUtils;
        private readonly DocumentsStorage _documentsStorage;
        public readonly QueryOperationContext QueryContext;

        public readonly UnmanagedBuffersPoolWithLowMemoryHandling UnmanagedBuffersPool;

        public Dictionary<string, FieldIndexing> DynamicFields;

        private readonly Func<string, SpatialField> _getSpatialField;

        /// [collection: [key: [referenceKeys]]]
        public Dictionary<string, Dictionary<Slice, HashSet<Slice>>> ReferencesByCollection;

        /// [collection: [key: [referenceKeys]]]
        public Dictionary<string, Dictionary<Slice, HashSet<Slice>>> ReferencesByCollectionForCompareExchange;

        [ThreadStatic]
        public static CurrentIndexingScope Current;

        public int CreatedFieldsCount;

        static CurrentIndexingScope()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => Current = null;
        }

        public readonly Index Index;

        public AbstractDynamicObject Source;

        public string SourceCollection;

        public readonly TransactionOperationContext IndexContext;

        public readonly IndexDefinitionBase IndexDefinition;

        public LuceneDocumentConverter CreateFieldConverter;

        public CurrentIndexingScope(Index index, DocumentsStorage documentsStorage, QueryOperationContext queryContext, IndexDefinitionBase indexDefinition, TransactionOperationContext indexContext, Func<string, SpatialField> getSpatialField, UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool)
        {
            _documentsStorage = documentsStorage;
            QueryContext = queryContext;
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
            _loadAttachmentStats = null;
            _loadCompareExchangeValueStats = null;
        }

        public unsafe List<DynamicAttachment> LoadAttachments(string documentId, IEnumerable<string> attachmentNames)
        {
            using (_loadAttachmentStats?.Start() ?? (_loadAttachmentStats = _stats?.For(IndexingOperation.LoadAttachment)))
            {
                var results = new List<DynamicAttachment>();
                foreach (var attachmentName in attachmentNames)
                {
                    var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(QueryContext.Documents, documentId, attachmentName, AttachmentType.Document, null);
                    if (attachment == null)
                        continue;

                    results.Add(new DynamicAttachment(attachment));
                }

                return results;
            }
        }

        public unsafe dynamic LoadAttachments(DynamicBlittableJson document)
        {
            if (document == null)
                return DynamicNullObject.Null;

            var id = GetSourceId(document);

            return LoadAttachments(id, GetAttachmentNames());

            IEnumerable<string> GetAttachmentNames()
            {
                var metadata = document[Constants.Documents.Metadata.Key] as DynamicBlittableJson;

                if (metadata == null)
                    yield break;

                var attachments = metadata[Constants.Documents.Metadata.Attachments] as DynamicArray;
                if (attachments == null)
                    yield break;

                foreach (dynamic attachmentMetadata in attachments)
                    yield return attachmentMetadata.Name;
            }
        }

        public unsafe dynamic LoadAttachment(string documentId, string attachmentName)
        {
            using (_loadAttachmentStats?.Start() ?? (_loadAttachmentStats = _stats?.For(IndexingOperation.LoadAttachment)))
            {
                if (attachmentName == null)
                    return DynamicNullObject.Null;

                var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(QueryContext.Documents, documentId, attachmentName, AttachmentType.Document, null);
                if (attachment == null)
                    return DynamicNullObject.Null;

                return new DynamicAttachment(attachment);
            }
        }

        public unsafe dynamic LoadAttachment(DynamicBlittableJson document, string attachmentName)
        {
            if (document == null)
                return DynamicNullObject.Null;

            var id = GetSourceId(document);

            return LoadAttachment(id, attachmentName);
        }

        public unsafe dynamic LoadDocument(LazyStringValue keyLazy, string keyString, string collectionName)
        {
            using (_loadDocumentStats?.Start() ?? (_loadDocumentStats = _stats?.For(IndexingOperation.LoadDocument)))
            {
                if (keyLazy == null && keyString == null)
                    return DynamicNullObject.Null;

                var source = Source;
                var id = GetSourceId(source);

                if (source is DynamicBlittableJson)
                {
                    if (keyLazy != null && id.Equals(keyLazy))
                        return source;

                    if (keyString != null && id.Equals(keyString))
                        return source;
                }

                if (TryGetKeySlice(keyLazy, keyString, out var keySlice) == false)
                    return DynamicNullObject.Null;

                // we intentionally don't dispose of the scope here, this is being tracked by the references
                // and will be disposed there.

                Slice.From(QueryContext.Documents.Allocator, id, out var idSlice);
                var references = GetReferencesForItem(idSlice);

                references.Add(keySlice);

                // when there is conflict, we need to apply same behavior as if the document would not exist
                var document = _documentsStorage.Get(QueryContext.Documents, keySlice, throwOnConflict: false);

                if (document == null)
                {
                    return DynamicNullObject.Null;
                }

                if (document.TryGetMetadata(out var metadata) && metadata.TryGet(Constants.Documents.Metadata.Collection, out string collection))
                {
                    if (string.Equals(collection, collectionName, StringComparison.OrdinalIgnoreCase) == false)
                        throw new InvalidOperationException($"Cannot load document '{keySlice}', because it's collection '{collection}' does not match the requested collection name '{collectionName}'.");
                }

                // we can't share one DynamicBlittableJson instance among all documents because we can have multiple LoadDocuments in a single scope
                return new DynamicBlittableJson(document);
            }
        }

        public unsafe dynamic LoadCompareExchangeValue(LazyStringValue keyLazy, string keyString)
        {
            using (_loadCompareExchangeValueStats?.Start() ?? (_loadCompareExchangeValueStats = _stats?.For(IndexingOperation.LoadCompareExchangeValue)))
            {
                if (keyLazy == null && keyString == null)
                    return DynamicNullObject.Null;

                var source = Source;
                var id = GetSourceId(source);

                if (TryGetCompareExchangeKeySlice(keyLazy, keyString, out var keySlice) == false)
                    return DynamicNullObject.Null;

                // we intentionally don't dispose of the scope here, this is being tracked by the references
                // and will be disposed there.
                Slice.From(QueryContext.Documents.Allocator, id, out var idSlice);
                var references = GetCompareExchangeReferencesForItem(idSlice);

                references.Add(keySlice);

                var value = _documentsStorage.DocumentDatabase.ServerStore.Cluster.GetCompareExchangeValue(QueryContext.Server, keySlice);

                if (value.Value == null || value.Value.TryGetMember(Constants.CompareExchange.ObjectFieldName, out object result) == false)
                    return DynamicNullObject.Null;

                if (result == null)
                    return DynamicNullObject.ExplicitNull;

                if (result is BlittableJsonReaderObject bjro)
                    return new DynamicBlittableJson(bjro);

                return result;
            }
        }

        private LazyStringValue GetSourceId(AbstractDynamicObject source)
        {
            if (source == null)
                throw new ArgumentException("Cannot execute Load. Source is not set.");

            var id = source.GetId() as LazyStringValue;
            if (id == null)
                throw new ArgumentException("Cannot execute Load. Source does not have a key.");

            return id;
        }

        private bool TryGetKeySlice(LazyStringValue keyLazy, string keyString, out Slice keySlice)
        {
            keySlice = default;

            // we intentionally don't dispose of the scope here, this is being tracked by the references
            // and will be disposed there.

            // making sure that we normalize the case of the key so we'll be able to find
            // it in case insensitive manner
            // In addition, special characters need to be escaped

            if (keyLazy != null)
            {
                if (keyLazy.Length == 0)
                    return false;

                DocumentIdWorker.GetSliceFromId(QueryContext.Documents, keyLazy, out keySlice);
            }
            else
            {
                if (keyString.Length == 0)
                    return false;

                DocumentIdWorker.GetSliceFromId(QueryContext.Documents, keyString, out keySlice);
            }

            return true;
        }

        private bool TryGetCompareExchangeKeySlice(LazyStringValue keyLazy, string keyString, out Slice keySlice)
        {
            keySlice = default;
            if (keyLazy != null)
            {
                if (keyLazy.Length == 0)
                    return false;

                var key = CompareExchangeKey.GetStorageKey(_documentsStorage.DocumentDatabase.Name, keyLazy);

                // we intentionally don't dispose of the scope here, this is being tracked by the references
                // and will be disposed there.
                Slice.From(QueryContext.Server.Allocator, key, out keySlice);
            }
            else
            {
                if (keyString.Length == 0)
                    return false;

                var key = CompareExchangeKey.GetStorageKey(_documentsStorage.DocumentDatabase.Name, keyString);

                // we intentionally don't dispose of the scope here, this is being tracked by the references
                // and will be disposed there.
                Slice.From(QueryContext.Server.Allocator, key, out keySlice);
            }

            return true;
        }

        public SpatialField GetOrCreateSpatialField(string name)
        {
            return _getSpatialField(name);
        }

        public void RegisterJavaScriptUtils(JavaScriptUtils javaScriptUtils)
        {
            if (_javaScriptUtils != null)
                return;

            _javaScriptUtils = javaScriptUtils;
            _javaScriptUtils.Reset(IndexContext);
        }

        public void Dispose()
        {
            _javaScriptUtils?.Clear();
            _javaScriptUtils = null;
            Current = null;
        }

        private HashSet<Slice> GetReferencesForItem(Slice key)
        {
            if (ReferencesByCollection == null)
                ReferencesByCollection = new Dictionary<string, Dictionary<Slice, HashSet<Slice>>>(StringComparer.OrdinalIgnoreCase);

            if (ReferencesByCollection.TryGetValue(SourceCollection, out Dictionary<Slice, HashSet<Slice>> referencesByCollection) == false)
                ReferencesByCollection.Add(SourceCollection, referencesByCollection = new Dictionary<Slice, HashSet<Slice>>(SliceComparer.Instance));

            if (referencesByCollection.TryGetValue(key, out HashSet<Slice> references) == false)
                referencesByCollection.Add(key, references = new HashSet<Slice>(SliceComparer.Instance));

            return references;
        }

        private HashSet<Slice> GetCompareExchangeReferencesForItem(Slice key)
        {
            if (ReferencesByCollectionForCompareExchange == null)
                ReferencesByCollectionForCompareExchange = new Dictionary<string, Dictionary<Slice, HashSet<Slice>>>(StringComparer.OrdinalIgnoreCase);

            if (ReferencesByCollectionForCompareExchange.TryGetValue(SourceCollection, out Dictionary<Slice, HashSet<Slice>> referencesByCollection) == false)
                ReferencesByCollectionForCompareExchange.Add(SourceCollection, referencesByCollection = new Dictionary<Slice, HashSet<Slice>>());

            if (referencesByCollection.TryGetValue(key, out HashSet<Slice> references) == false)
                referencesByCollection.Add(key, references = new HashSet<Slice>(SliceComparer.Instance));

            return references;
        }
    }
}
