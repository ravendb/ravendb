using Raven.Client;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxJavascriptDocumentConverterJint : CoraxJavascriptDocumentConverterBase<JsHandleJint>
{
    private JintEngineEx _engineEx => EngineHandle as JintEngineEx;

    public CoraxJavascriptDocumentConverterJint(MapIndex index, bool storeValue = false)
        : base(index, index.Definition.IndexDefinition, storeValue, false, true, 1, null, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
    {
    }

    public CoraxJavascriptDocumentConverterJint(MapReduceIndex index, bool storeValue = false)
        : base(index, index.Definition.IndexDefinition, storeValue, false, true, 1, null, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
    {
    }

    protected override object GetBlittableSupportedType(JsHandleJint val, bool flattenArrays, bool forIndexing, JsonOperationContext indexContext)
    {
        return TypeConverter.ToBlittableSupportedType(val, _engineEx, flattenArrays: false, forIndexing: true, indexContext);
    }
}
