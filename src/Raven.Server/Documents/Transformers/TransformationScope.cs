using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.Documents.Transformers
{
    public class TransformationScope : IDisposable
    {
        private readonly IndexingFunc _transformResults;
        private readonly DocumentsOperationContext _context;

        public TransformationScope(IndexingFunc transformResults, DocumentsStorage documentsStorage, DocumentsOperationContext context)
        {
            _transformResults = transformResults;
            _context = context;
            CurrentTransformationScope.Current = new CurrentTransformationScope(documentsStorage, context);
        }

        public void Dispose()
        {
            CurrentTransformationScope.Current = null;
        }

        public IEnumerable<Document> Transform(IEnumerable<Document> documents)
        {
            var docsEnumerator = new StaticIndexDocsEnumerator(documents, _transformResults, null, StaticIndexDocsEnumerator.EnumerationType.Transformer);

            IEnumerable transformedResults;
            while (docsEnumerator.MoveNext(out transformedResults))
            {
                using (docsEnumerator.Current.Data)
                {
                    var values = new DynamicJsonArray();
                    var result = new DynamicJsonValue
                    {
                        ["$values"] = values
                    };

                    PropertyAccessor accessor = null;
                    foreach (var transformedResult in transformedResults)
                    {
                        if (accessor == null)
                            accessor = PropertyAccessor.Create(transformedResult.GetType());

                        var value = new DynamicJsonValue();
                        foreach (var property in accessor.Properties)
                        {
                            var propertyValue = property.Value(transformedResult);
                            var propertyValueAsEnumerable = propertyValue as IEnumerable<object>;
                            if (propertyValueAsEnumerable != null)
                            {
                                value[property.Key] = new DynamicJsonArray(propertyValueAsEnumerable.Select(x =>
                                {
                                    var dynamicDocument = x as DynamicDocumentObject;
                                    if (dynamicDocument != null)
                                        return (BlittableJsonReaderObject)dynamicDocument;

                                    return x;
                                }));
                                continue;
                            }

                            value[property.Key] = propertyValue;
                        }

                        values.Add(value);
                    }

                    yield return new Document
                    {
                        //Key = docsEnumerator.Current.Key,
                        Data = _context.ReadObject(result, docsEnumerator.Current.Key),
                        Etag = docsEnumerator.Current.Etag,
                        StorageId = docsEnumerator.Current.StorageId
                    };
                }
            }
        }
    }
}