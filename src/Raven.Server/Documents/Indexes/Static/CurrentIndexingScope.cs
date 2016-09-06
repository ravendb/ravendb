using System;
using System.Collections.Generic;
using Raven.Client.Linq;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Indexes.Static
{
    public class CurrentIndexingScope : IDisposable
    {
        private readonly DocumentsStorage _documentsStorage;
        private readonly DocumentsOperationContext _documentsContext;

        private DynamicBlittableJson _document;

        /// [collection: [key: [referenceKeys]]]
        public Dictionary<string, Dictionary<string, HashSet<Slice>>> ReferencesByCollection;

        /// [collection: [collectionKey: etag]]
        public Dictionary<string, Dictionary<Slice, long>> ReferenceEtagsByCollection;

        [ThreadStatic]
        public static CurrentIndexingScope Current;

        public dynamic Source;

        public string SourceCollection;

        public CurrentIndexingScope(DocumentsStorage documentsStorage, DocumentsOperationContext documentsContext)
        {
            _documentsStorage = documentsStorage;
            _documentsContext = documentsContext;
        }

        public unsafe dynamic LoadDocument(LazyStringValue keyLazy, string keyString, string collectionName)
        {
            if (keyLazy == null && keyString == null)
                return DynamicNullObject.Null;

            var source = Source;
            if (source == null)
                throw new ArgumentException("Cannot execute LoadDocument. Source is not set.");

            var id = source.__document_id as LazyStringValue;
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

                keySlice = Slice.External(_documentsContext.Allocator, keyLazy.Buffer, keyLazy.Size);
            }
            else
            {
                if (keyString.Length == 0)
                    return DynamicNullObject.Null;

                keySlice = Slice.From(_documentsContext.Allocator, keyString);
            }

            // making sure that we normalize the case of the key so we'll be able to find
            // it in case insensitive manner
            _documentsContext.Allocator.ToLowerCase(ref keySlice.Content);

            var collectionSlice = Slice.From(_documentsContext.Allocator, collectionName, ByteStringType.Immutable);

            var references = GetReferencesForDocument(id);
            var referenceEtags = GetReferenceEtags();

            references.Add(keySlice);

            var document = _documentsStorage.Get(_documentsContext, keySlice);
            if (document == null)
            {
                MaybeUpdateReferenceEtags(referenceEtags, collectionSlice, 0);
                return DynamicNullObject.Null;
            }

            MaybeUpdateReferenceEtags(referenceEtags, collectionSlice, document.Etag);

            if (_document == null)
            {
                _document = new DynamicBlittableJson(document);
            }
            else
            {
                _document.Set(document);
            }

            return _document;
        }

        public void Dispose()
        {
            Current = null;
        }

        private static void MaybeUpdateReferenceEtags(Dictionary<Slice, long> referenceEtags, Slice collection, long etag)
        {
            long oldEtag;
            if (referenceEtags.TryGetValue(collection, out oldEtag) == false)
            {
                referenceEtags[collection] = etag;
                return;
            }

            if (oldEtag >= etag)
                return;

            referenceEtags[collection] = etag;
        }

        private Dictionary<Slice, long> GetReferenceEtags()
        {
            if (ReferenceEtagsByCollection == null)
                ReferenceEtagsByCollection = new Dictionary<string, Dictionary<Slice, long>>(StringComparer.OrdinalIgnoreCase);

            Dictionary<Slice, long> referenceEtags;
            if (ReferenceEtagsByCollection.TryGetValue(SourceCollection, out referenceEtags) == false)
                ReferenceEtagsByCollection.Add(SourceCollection, referenceEtags = new Dictionary<Slice, long>(SliceComparer.Instance));

            return referenceEtags;
        }

        private HashSet<Slice> GetReferencesForDocument(string key)
        {
            if (ReferencesByCollection == null)
                ReferencesByCollection = new Dictionary<string, Dictionary<string, HashSet<Slice>>>();

            Dictionary<string, HashSet<Slice>> referencesByCollection;
            if (ReferencesByCollection.TryGetValue(SourceCollection, out referencesByCollection) == false)
                ReferencesByCollection.Add(SourceCollection, referencesByCollection = new Dictionary<string, HashSet<Slice>>());

            HashSet<Slice> references;
            if (referencesByCollection.TryGetValue(key, out references) == false)
                referencesByCollection.Add(key, references = new HashSet<Slice>(SliceComparer.Instance));

            return references;
        }
    }
}