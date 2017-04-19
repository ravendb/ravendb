using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public delegate IEnumerable IndexingFunc(IEnumerable<dynamic> items); 

    public abstract class StaticIndexBase
    {
        private LuceneDocumentConverter _createFieldsConverter;

        private readonly Dictionary<string, CollectionName> _collectionsCache = new Dictionary<string, CollectionName>(StringComparer.OrdinalIgnoreCase);

        public readonly Dictionary<string, List<IndexingFunc>> Maps = new Dictionary<string, List<IndexingFunc>>(StringComparer.OrdinalIgnoreCase);

        public readonly Dictionary<string, HashSet<CollectionName>> ReferencedCollections = new Dictionary<string, HashSet<CollectionName>>(StringComparer.OrdinalIgnoreCase);

        public bool HasDynamicFields { get; set; }

        public bool HasBoostedFields { get; set; }

        public string Source;

        public void AddMap(string collection, IndexingFunc map)
        {
            List<IndexingFunc> funcs;
            if (Maps.TryGetValue(collection, out funcs) == false)
                Maps[collection] = funcs = new List<IndexingFunc>();

            funcs.Add(map);
        }

        public void AddReferencedCollection(string collection, string referencedCollection)
        {
            CollectionName referencedCollectionName;
            if (_collectionsCache.TryGetValue(referencedCollection, out referencedCollectionName) == false)
                _collectionsCache[referencedCollection] = referencedCollectionName = new CollectionName(referencedCollection);

            HashSet<CollectionName> set;
            if (ReferencedCollections.TryGetValue(collection, out set) == false)
                ReferencedCollections[collection] = set = new HashSet<CollectionName>();

            set.Add(referencedCollectionName);
        }

        public IEnumerable<dynamic> Recurse(object item, Func<dynamic, dynamic> func)
        {
            return new RecursiveFunction(item, func).Execute();
        }

        public dynamic LoadDocument<TIGnored>(object keyOrEnumerable, string collectionName)
        {
            return LoadDocument(keyOrEnumerable, collectionName);
        }

        public dynamic LoadDocument(object keyOrEnumerable, string collectionName)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException(
                    "Indexing scope was not initialized. Key: " + keyOrEnumerable);

            var keyLazy = keyOrEnumerable as LazyStringValue;
            if (keyLazy != null)
                return CurrentIndexingScope.Current.LoadDocument(keyLazy, null, collectionName);

            var keyString = keyOrEnumerable as string;
            if (keyString != null)
                return CurrentIndexingScope.Current.LoadDocument(null, keyString, collectionName);

            var enumerable = keyOrEnumerable as IEnumerable;
            if (enumerable != null)
            {
                var enumerator = enumerable.GetEnumerator();
                using (enumerable as IDisposable)
                {
                    var items = new List<dynamic>();
                    while (enumerator.MoveNext())
                    {
                        items.Add(LoadDocument(enumerator.Current, collectionName));
                    }
                    return new DynamicArray(items);
                }
            }

            throw new InvalidOperationException(
                "LoadDocument may only be called with a string or an enumerable, but was called with a parameter of type " +
                keyOrEnumerable.GetType().FullName + ": " + keyOrEnumerable);
        }

        private struct StaticIndexLuceneDocumentWrapper : ILuceneDocumentWrapper
        {
            private readonly List<AbstractField> _fields;

            public StaticIndexLuceneDocumentWrapper(List<AbstractField> fields)
            {
                _fields = fields;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Add(AbstractField field)
            {
                _fields.Add(field);                
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IList<IFieldable> GetFields()
            {
                throw new NotImplementedException();
            }
        }

        protected IEnumerable<AbstractField> CreateField(string name, object value, bool stored = false, bool? analyzed = null)
        {
            // IMPORTANT: Do not delete this method, it is used by the indexes code when using LoadDocument
            FieldIndexing? index;

            switch (analyzed)
            {
                case true:
                    index = FieldIndexing.Analyzed;
                    break;
                case false:
                    index = FieldIndexing.NotAnalyzed;
                    break;
                default:
                    index = null;
                    break;
            }

            var field = IndexField.Create(name, new IndexFieldOptions
            {
                Storage = stored ? FieldStorage.Yes : FieldStorage.No,
                TermVector = FieldTermVector.No,
                Indexing = index
            }, null);

            if (_createFieldsConverter == null)
                _createFieldsConverter = new LuceneDocumentConverter(new IndexField[] { });

            var result = new List<AbstractField>();
            _createFieldsConverter.GetRegularFields(new StaticIndexLuceneDocumentWrapper(result), field, value, CurrentIndexingScope.Current.IndexContext);
            return result;
        }

        public IndexingFunc Reduce;

        public string[] OutputFields;

        public string[] GroupByFields;


        public dynamic MetadataFor(dynamic doc)
        {
            var json = (DynamicBlittableJson)doc;
            json.EnsureMetadata();
            return doc[Constants.Documents.Metadata.Key];
        }

        public dynamic AsJson(dynamic doc)
        {
            var json = (DynamicBlittableJson)doc;
            json.EnsureMetadata();
            return json;
        }
    }
}