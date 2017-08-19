using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public sealed class LuceneDocumentConverter : LuceneDocumentConverterBase
    {
        private readonly BlittableJsonTraverser _blittableTraverser;

        public LuceneDocumentConverter(ICollection<IndexField> fields, bool reduceOutput = false)
            : base(fields, reduceOutput)
        {
            _blittableTraverser = reduceOutput ? BlittableJsonTraverser.FlatMapReduceResults : BlittableJsonTraverser.Default;
        }
        
        protected override int GetFields<T>(T instance, LazyStringValue key, object doc, JsonOperationContext indexContext) 
        {
            int newFields = 0; 

            var document = (Document)doc;
            if (key != null)
            {
                Debug.Assert(document.LowerId == null || (key == document.LowerId));

                instance.Add(GetOrCreateKeyField(key));
                newFields++;
            }

            if (_reduceOutput)
            {
                instance.Add(GetReduceResultValueField(document.Data));
                newFields++;
            }

            foreach (var indexField in _fields.Values)
            {
                if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, indexField.OriginalName ?? indexField.Name, out object value) == false)
                    continue;

                newFields += GetRegularFields(instance, indexField, value, indexContext);
            }

            return newFields;
        }
    }
}
