using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL;

public abstract class EtlTransformerV8<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation> : EtlTransformer<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation, JsHandleV8>
    where TExtracted : ExtractedItem
    where TStatsScope : AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation>
    where TEtlPerformanceOperation : EtlPerformanceOperation
{
    protected EtlTransformerV8(DocumentDatabase database, DocumentsOperationContext context, PatchRequest mainScript)
        : base(database, context, mainScript)
    {
    }

    public override ReturnRun CreateDocumentScriptRunner(bool debugMode, out SingleRun<JsHandleV8> documentScript)
    {
        return EtlTransformerHelper.CreateDocumentScriptRunnerV8(Database, _mainScript, debugMode, out documentScript);
    }

    protected override ScriptRunnerResult<JsHandleV8> CreateScriptRunnerResult(object obj)
    {
        return EtlTransformerHelper.GetScriptRunnerResultV8(obj, DocumentScript, EngineHandle);
    }
}