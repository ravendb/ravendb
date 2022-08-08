using Raven.Client;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxJavascriptDocumentConverterV8 : CoraxJavascriptDocumentConverterBase<JsHandleV8>
{
    private V8EngineEx _engineEx => EngineHandle as V8EngineEx;

    public CoraxJavascriptDocumentConverterV8(MapIndex index, bool storeValue = false)
        : base(index, index.Definition.IndexDefinition, storeValue, false, true, 1, null, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
    {
    }

    public CoraxJavascriptDocumentConverterV8(MapReduceIndex index, bool storeValue = false)
        : base(index, index.Definition.IndexDefinition, storeValue, false, true, 1, null, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
    {
    }

    protected override object GetBlittableSupportedType(JsHandleV8 val, bool flattenArrays, bool forIndexing, JsonOperationContext indexContext)
    {
        return TypeConverter.ToBlittableSupportedType(val, _engineEx, flattenArrays: false, forIndexing: true, indexContext);
    }
}
