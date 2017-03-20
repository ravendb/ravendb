using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Raven.Server.Documents.Includes;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Transformers
{
    public class TransformationScope : IDisposable
    {
        private readonly TransformerBase _transformer;
        private readonly DocumentsOperationContext _context;
        private readonly bool _nested;

        public TransformationScope(TransformerBase transformer, BlittableJsonReaderObject transformerParameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, TransformerStore transformerStore, DocumentsOperationContext context, bool nested)
        {
            _transformer = transformer;
            _context = context;
            _nested = nested;
            if (_nested == false)
            {
                Debug.Assert(CurrentTransformationScope.Current == null);
                CurrentTransformationScope.Current = new CurrentTransformationScope(transformerParameters, include, documentsStorage, transformerStore, context);
            }
            else
                Debug.Assert(CurrentTransformationScope.Current != null);
        }

        public bool HasLoadedAnyDocument => LoadedDocumentEtags != null && LoadedDocumentEtags.Count > 0;

        public List<long> LoadedDocumentEtags => CurrentTransformationScope.Current.LoadedDocumentEtags;

        public void Dispose()
        {
            if (_nested == false)
                CurrentTransformationScope.Current = null;
        }

        public IEnumerable<dynamic> Transform(IEnumerable<dynamic> items)
        {
            foreach (var item in _transformer.TransformResults(items))
            {
                yield return item;
            }
        }

        public IEnumerable<Document> Transform(IEnumerable<Document> documents)
        {
            if (_transformer.HasGroupBy == false)
            {
                var docsEnumerator = new TransformedDocsEnumerator(documents, _transformer.TransformResults);

                IEnumerable transformedResults;
                while (docsEnumerator.MoveNext(out transformedResults))
                {
                    if (docsEnumerator.Current == null)
                    {
                        yield return Document.ExplicitNull;
                        continue;
                    }

                    using (docsEnumerator.Current.Data)
                    {
                        var values = new DynamicJsonArray();
                        var result = new DynamicJsonValue
                        {
                            ["$values"] = values
                        };

                        foreach (var transformedResult in transformedResults)
                        {
                            var value = TypeConverter.ToBlittableSupportedType(transformedResult);
                            values.Add(value);
                        }

                        var document = new Document
                        {
                            Key = docsEnumerator.Current.Key,
                            Data = _context.ReadObject(result, docsEnumerator.Current.Key ?? string.Empty),
                            Etag = docsEnumerator.Current.Etag,
                            StorageId = docsEnumerator.Current.StorageId
                        };

                        yield return document;
                    }
                }
            }
            else
            {
                var groupByEnumerationWrapper = new GroupByTransformationWrapper(documents);

                var values = new DynamicJsonArray();
                var result = new DynamicJsonValue
                {
                    ["$values"] = values
                };

                foreach (var transformedResult in _transformer.TransformResults(groupByEnumerationWrapper))
                {
                    if (transformedResult == null)
                    {
                        yield return Document.ExplicitNull;
                        continue;
                    }

                    var value = TypeConverter.ToBlittableSupportedType(transformedResult);
                    values.Add(value);
                }

                var document = new Document
                {
                    Data = _context.ReadObject(result, string.Empty),
                };

                yield return document;
            }
        }

        private class GroupByTransformationWrapper : IEnumerable<DynamicBlittableJson>
        {
            private readonly Enumerator _enumerator = new Enumerator();

            public GroupByTransformationWrapper(IEnumerable<Document> docs)
            {
                _enumerator.Initialize(docs.GetEnumerator());
            }

            public IEnumerator<DynamicBlittableJson> GetEnumerator()
            {
                return _enumerator;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class Enumerator : IEnumerator<DynamicBlittableJson>
            {
                private IEnumerator<Document> _items;

                public void Initialize(IEnumerator<Document> items)
                {
                    _items = items;
                }

                public bool MoveNext()
                {
                    if (_items.MoveNext() == false)
                        return false;

                    Current = new DynamicBlittableJson(_items.Current); // we have to create new instance to properly GroupBy

                    CurrentTransformationScope.Current.Source = Current;
                    return true;
                }

                public void Reset()
                {
                    throw new NotImplementedException();
                }

                public DynamicBlittableJson Current { get; private set; }

                object IEnumerator.Current => Current;

                public void Dispose()
                {

                }
            }
        }
    }
}