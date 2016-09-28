using System.Collections.Generic;
using System.Diagnostics;
using Lucene.Net.Documents;
using Raven.Server.Json;
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
                _blittableTraverser = new BlittableJsonTraverser(new char[] {}); // map-reduce results have always flat structure
            else
                _blittableTraverser = BlittableJsonTraverser.Default;
        }

        protected override IEnumerable<AbstractField> GetFields(LazyStringValue key, object doc, JsonOperationContext indexContext)
        {
            var document = (Document)doc;
            if (key != null)
            {
                Debug.Assert(document.LoweredKey == null || (key == document.LoweredKey));

                yield return GetOrCreateKeyField(key);
            }

            if (_reduceOutput)
            {
                yield return GetReduceResultValueField(document.Data);
            }

            foreach (var indexField in _fields.Values)
            {
                object value;
                BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, indexField.Name, out value);

                foreach (var luceneField in GetRegularFields(indexField, value, indexContext))
                    yield return luceneField;
            }
        }
    }
}