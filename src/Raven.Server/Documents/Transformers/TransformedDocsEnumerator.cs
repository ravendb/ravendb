using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Transformers
{
    public class TransformedDocsEnumerator : StaticIndexDocsEnumerator
    {
        public TransformedDocsEnumerator(IEnumerable<Document> docs, IndexingFunc func) : base(docs)
        {
            _enumerationType = EnumerationType.Transformer;
            _resultsOfCurrentDocument = func(new DynamicIteratonOfCurrentDocumentWrapper(this));
        }
    }
}