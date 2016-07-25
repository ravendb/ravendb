using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using System.Linq;
using Raven.Server.Documents.Includes;
using Sparrow.Json;

namespace Raven.Server.Documents.Transformers
{
    public class TransformationScope : IDisposable
    {
        private readonly IndexingFunc _transformer;
        private readonly DocumentsOperationContext _context;

        public TransformationScope(IndexingFunc transformer, BlittableJsonReaderObject transformerParameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, DocumentsOperationContext context)
        {
            _transformer = transformer;
            _context = context;
            CurrentTransformationScope.Current = new CurrentTransformationScope(transformerParameters, include, documentsStorage, context);
        }

        public void Dispose()
        {
            CurrentTransformationScope.Current = null;
        }

        public IEnumerable<Document> Transform(IEnumerable<Document> documents)
        {
            var docsEnumerator = new StaticIndexDocsEnumerator(documents, _transformer, null, StaticIndexDocsEnumerator.EnumerationType.Transformer);

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
                                value[property.Key] = new DynamicJsonArray(propertyValueAsEnumerable.Select(ConvertType));
                                continue;
                            }

                            value[property.Key] = ConvertType(propertyValue);
                        }

                        values.Add(value);
                    }

                    var document = new Document
                    {
                        //Key = docsEnumerator.Current.Key,
                        Data = _context.ReadObject(result, docsEnumerator.Current.Key ?? string.Empty),
                        Etag = docsEnumerator.Current.Etag,
                        StorageId = docsEnumerator.Current.StorageId
                    };

                    yield return document;
                }
            }
        }

        private static object ConvertType(object value)
        {
            if (value == null)
                return null;

            var dynamicDocument = value as DynamicDocumentObject;
            if (dynamicDocument != null)
                return (BlittableJsonReaderObject)dynamicDocument;

            var transformerParameter = value as TransformerParameter;
            if (transformerParameter != null)
                return transformerParameter.OriginalValue;

            return value;
        }
    }
}