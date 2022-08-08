using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public sealed class LuceneJavascriptDocumentConverterJint : LuceneJavascriptDocumentConverterBase<JsHandleJint>
    {
        private JintEngineEx _engineEx => EngineHandle as JintEngineEx;
        public LuceneJavascriptDocumentConverterJint(MapIndex index, bool storeValue = false) : base(index, index.Definition.IndexDefinition, storeValue: storeValue)
        {
        }

        public LuceneJavascriptDocumentConverterJint(MapReduceIndex index, bool storeValue = false) : base(index, index.Definition.IndexDefinition, storeValue: storeValue)
        {
        }

        protected override object GetBlittableSupportedType(JsHandleJint val, bool flattenArrays, bool forIndexing, JsonOperationContext indexContext)
        {
            return TypeConverter.ToBlittableSupportedType(val, _engineEx, flattenArrays: false, forIndexing: true, indexContext);
        }
    }
}
