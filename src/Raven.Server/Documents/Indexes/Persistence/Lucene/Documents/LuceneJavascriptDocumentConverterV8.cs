using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public sealed class LuceneJavascriptDocumentConverterV8 : LuceneJavascriptDocumentConverterBase<JsHandleV8>
    {
        private V8EngineEx _engineEx => EngineHandle as V8EngineEx;

        public LuceneJavascriptDocumentConverterV8(MapIndex index, bool storeValue = false) : base(index, index.Definition.IndexDefinition, storeValue: storeValue)
        {
        }

        public LuceneJavascriptDocumentConverterV8(MapReduceIndex index, bool storeValue = false) : base(index, index.Definition.IndexDefinition, storeValue: storeValue)
        {
        }

        protected override object GetBlittableSupportedType(JsHandleV8 val, bool flattenArrays, bool forIndexing, JsonOperationContext indexContext)
        {
            return TypeConverter.ToBlittableSupportedType(val, _engineEx, flattenArrays: false, forIndexing: true, indexContext);
        }
    }
}
