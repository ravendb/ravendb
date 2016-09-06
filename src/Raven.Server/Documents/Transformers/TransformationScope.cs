using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using System.Linq;
using Raven.Server.Documents.Includes;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Transformers
{
    public class TransformationScope : IDisposable
    {
        private readonly IndexingFunc _transformer;
        private readonly DocumentsOperationContext _context;

        public TransformationScope(IndexingFunc transformer, BlittableJsonReaderObject transformerParameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, TransformerStore transformerStore, DocumentsOperationContext context, bool nested)
        {
            _transformer = transformer;
            _context = context;
            if (nested == false)
            {
                Debug.Assert(CurrentTransformationScope.Current == null);
                CurrentTransformationScope.Current = new CurrentTransformationScope(transformerParameters, include, documentsStorage, transformerStore, context);
            }
            else
                Debug.Assert(CurrentTransformationScope.Current != null);
        }

        public void Dispose()
        {
            CurrentTransformationScope.Current = null;
        }

        public IEnumerable<dynamic> Transform(IEnumerable<dynamic> items)
        {
            foreach (var item in _transformer(items))
            {
                yield return item;
            }
        }

        public IEnumerable<Document> Transform(IEnumerable<Document> documents)
        {
            var docsEnumerator = new StaticIndexDocsEnumerator(documents, _transformer, null, StaticIndexDocsEnumerator.EnumerationType.Transformer);

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
                        var value = TypeConverter.ConvertType(transformedResult, _context);
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
    }
}