using System.Collections.Generic;
using System.Diagnostics;

using Lucene.Net.Documents;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public class LuceneDocumentConverter : LuceneDocumentConverterBase
    {
        private readonly BlittableJsonTraverser _blittableTraverser;

        public LuceneDocumentConverter(ICollection<IndexField> fields, bool reduceOutput = false)
            : base(fields, reduceOutput)
        {
            if (reduceOutput)
                _blittableTraverser = new BlittableJsonTraverser(new char[] { }); // map-reduce results are have always flat structure
            else
                _blittableTraverser = new BlittableJsonTraverser();
        }

        protected override IEnumerable<AbstractField> GetFields(LazyStringValue key, object doc)
        {
            var document = (Document)doc;
            if (document.Key != null)
            {
                Debug.Assert(key == document.Key);

                yield return GetOrCreateKeyField(document.Key);
            }

            foreach (var indexField in _fields.Values)
            {
                object value;

                if (_blittableTraverser.TryRead(document.Data, indexField.Name, out value) == false)
                    continue;

                foreach (var luceneField in GetRegularFields(indexField, value))
                    yield return luceneField;
            }
        }
    }
}