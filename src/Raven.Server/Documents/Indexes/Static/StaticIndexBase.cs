using System;
using System.Collections;
using System.Collections.Generic;
using Lucene.Net.Documents;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Static
{
    public delegate IEnumerable IndexingFunc(IEnumerable<dynamic> items); 

    public abstract class StaticIndexBase
    {
        private LuceneDocumentConverter _createFieldsConverter;

        public readonly Dictionary<string, IndexingFunc> Maps = new Dictionary<string, IndexingFunc>(StringComparer.OrdinalIgnoreCase);

        public readonly Dictionary<string, HashSet<string>> ReferencedCollections = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        public string Source;

        public void AddMap(string collection, IndexingFunc map)
        {
            Maps[collection] = map;
        }

        public void AddReferencedCollection(string collection, string referencedCollection)
        {
            HashSet<string> set;
            if (ReferencedCollections.TryGetValue(collection, out set) == false)
                ReferencedCollections[collection] = set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            set.Add(referencedCollection);
        }

        public IEnumerable<dynamic> Recurse(object item, Func<dynamic, dynamic> func)
        {
            return new RecursiveFunction(item, func).Execute();
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

        protected IEnumerable<AbstractField> CreateField(string name, object value, bool stored = false, bool? analyzed = null)
        {
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
                _createFieldsConverter = new LuceneDocumentConverter(new IndexField[] {});

            return _createFieldsConverter.GetRegularFields(field, value, null);
        }

        public IndexingFunc Reduce;

        public string[] OutputFields;

        public string[] GroupByFields;
    }
}