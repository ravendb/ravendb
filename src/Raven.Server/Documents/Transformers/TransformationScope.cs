using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Transformers
{
    public class TransformationScope : IDisposable
    {
        private readonly DocumentsOperationContext _context;

        public TransformationScope(IndexingFunc transformResults, DocumentDatabase documentDatabase, DocumentsOperationContext context)
        {
            _context = context;
            CurrentTransformationScope.Current = new CurrentTransformationScope(transformResults, documentDatabase);
        }

        public void Dispose()
        {
            CurrentTransformationScope.Current = null;
        }

        public IEnumerable<Document> Transform(IEnumerable<Document> documents)
        {
            var docsEnumerator = new StaticIndexDocsEnumerator(documents, CurrentTransformationScope.Current.TransformResults, null, StaticIndexDocsEnumerator.EnumerationType.Transformer);

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
                            value[property.Key] = property.Value(transformedResult);

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