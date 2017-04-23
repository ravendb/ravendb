using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Json;
using Sparrow.Json;
using LuceneDocument = Lucene.Net.Documents.Document;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public sealed class LuceneDocumentConverter : LuceneDocumentConverterBase
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
        
        protected override int GetFields<T>(T instance, LazyStringValue key, object doc, JsonOperationContext indexContext) 
        {
            int newFields = 0; 

            var document = (Document)doc;
            if (key != null)
            {
                Debug.Assert(document.LoweredKey == null || (key == document.LoweredKey));

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
                object value;
                if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, indexField.Name, out value) == false)
                    continue;

                newFields += GetRegularFields(instance, indexField, value, indexContext);
            }

            return newFields;
        }
    }
}