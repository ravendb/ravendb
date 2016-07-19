using System.Collections.Generic;
using System.Diagnostics;

using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Sparrow.Binary;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public class LuceneDocumentConverter : LuceneDocumentConverterBase
    {
        private readonly BlittableJsonTraverser _blittableTraverser;
        private readonly Field _reduceValueField = new Field(Constants.ReduceValueFieldName, new byte[0], 0, 0, Field.Store.YES);

        private byte[] _reduceValueBuffer;

        public LuceneDocumentConverter(ICollection<IndexField> fields, bool reduceOutput = false)
            : base(fields, reduceOutput)
        {
            if (reduceOutput)
            {
                _blittableTraverser = new BlittableJsonTraverser(new char[] { }); // map-reduce results have always flat structure
                _reduceValueBuffer = new byte[0];
            }
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

            if (_reduceOutput)
            {
                _reduceValueField.SetValue(GetReduceResult(document.Data), 0, document.Data.Size);
                yield return _reduceValueField;
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

        private byte[] GetReduceResult(BlittableJsonReaderObject reduceResult)
        {
            var necessarySize = Bits.NextPowerOf2(reduceResult.Size);

            if (_reduceValueBuffer.Length < necessarySize)
                _reduceValueBuffer = new byte[necessarySize];

            unsafe
            {
                fixed (byte* v = _reduceValueBuffer)
                    reduceResult.CopyTo(v);
            }

            return _reduceValueBuffer;
        }
    }
}