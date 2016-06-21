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

        private DynamicDocumentObject _document;

        private DynamicNullObject _null;

        public HashSet<string> ReferencedCollections;

        /// [collection: [key: [referenceKeys]]]
        public Dictionary<string, Dictionary<string, HashSet<Slice>>> ReferencesByCollection;

        /// [collection: [referenceKey: etag]]
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
                return Null();

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
                keySlice = Slice.External(_documentsContext.Allocator, keyLazy.Buffer, keyLazy.Size);
            else
                keySlice = Slice.From(_documentsContext.Allocator, keyString, ByteStringType.Immutable);

            var referencedCollections = GetReferencedCollections();
            var references = GetReferencesForDocument(id);
            var referenceEtags = GetReferenceEtags();

            referencedCollections.Add(collectionName);
            references.Add(keySlice);

            var document = _documentsStorage.Get(_documentsContext, keyString ?? keySlice.ToString()); // TODO [ppekrol] fix me
            if (document == null)
            {
                referenceEtags[keySlice] = 0;
                return Null();
            }

            referenceEtags[keySlice] = document.Etag;

            if (_document == null)
                _document = new DynamicDocumentObject();

            _document.Set(document);

            return _document;
        }

        public void Dispose()
        {
            Current = null;
        }

        private DynamicNullObject Null()
        {
            return _null ?? (_null = new DynamicNullObject());
        }

        private HashSet<string> GetReferencedCollections()
        {
            return ReferencedCollections ?? (ReferencedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase));
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