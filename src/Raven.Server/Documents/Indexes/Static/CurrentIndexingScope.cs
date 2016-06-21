using System;
using System.Collections.Generic;
using Raven.Client.Linq;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public class CurrentIndexingScope : IDisposable
    {
        private readonly DocumentsStorage _documentsStorage;
        private readonly DocumentsOperationContext _documentsContext;

        private DynamicDocumentObject _document;

        private DynamicNullObject _null;

        public HashSet<string> ReferencedCollections;

        public Dictionary<string, Dictionary<string, HashSet<string>>> ReferencesByCollection;

        public Dictionary<string, Dictionary<string, long>> ReferenceEtagsByCollection;

        [ThreadStatic]
        public static CurrentIndexingScope Current;

        public dynamic Source;

        public string SourceCollection;

        public CurrentIndexingScope(DocumentsStorage documentsStorage, DocumentsOperationContext documentsContext)
        {
            _documentsStorage = documentsStorage;
            _documentsContext = documentsContext;
        }

        public dynamic LoadDocument(LazyStringValue keyLazy, string keyString, string collectionName)
        {
            if (keyLazy == null && keyString == null)
                return Null();

            var source = Source;
            if (source == null)
                throw new ArgumentException("Cannot execute LoadDocument. Source is not set.");

            var id = source.__document_id as LazyStringValue;
            if (id == null)
                throw new ArgumentException("Cannot execute LoadDocument. Source does not have a key.");

            var key = keyLazy ?? keyString;
            if (id.Equals(key))
                return source;

            var referencedCollections = GetReferencedCollections();
            var references = GetReferencesForDocument(id);
            var referenceEtags = GetReferenceEtags();

            referencedCollections.Add(collectionName);
            references.Add(key);

            var document = _documentsStorage.Get(_documentsContext, key);
            if (document == null)
            {
                referenceEtags[key] = 0;
                return Null();
            }

            referenceEtags[key] = document.Etag;

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

        private Dictionary<string, long> GetReferenceEtags()
        {
            if (ReferenceEtagsByCollection == null)
                ReferenceEtagsByCollection = new Dictionary<string, Dictionary<string, long>>(StringComparer.OrdinalIgnoreCase);

            Dictionary<string, long> referenceEtags;
            if (ReferenceEtagsByCollection.TryGetValue(SourceCollection, out referenceEtags) == false)
                ReferenceEtagsByCollection.Add(SourceCollection, referenceEtags = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase));

            return referenceEtags;
        }

        private HashSet<string> GetReferencesForDocument(string key)
        {
            if (ReferencesByCollection == null)
                ReferencesByCollection = new Dictionary<string, Dictionary<string, HashSet<string>>>();

            Dictionary<string, HashSet<string>> referencesByCollection;
            if (ReferencesByCollection.TryGetValue(SourceCollection, out referencesByCollection) == false)
                ReferencesByCollection.Add(SourceCollection, referencesByCollection = new Dictionary<string, HashSet<string>>());

            HashSet<string> references;
            if (referencesByCollection.TryGetValue(key, out references) == false)
                referencesByCollection.Add(key, references = new HashSet<string>(StringComparer.OrdinalIgnoreCase));

            return references;
        }
    }
}