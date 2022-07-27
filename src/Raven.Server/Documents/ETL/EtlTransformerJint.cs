using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL;

public abstract class EtlTransformerJint<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation> : EtlTransformer<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation, JsHandleJint>
    where TExtracted : ExtractedItem
    where TStatsScope : AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation>
    where TEtlPerformanceOperation : EtlPerformanceOperation
{
    protected EtlTransformerJint(DocumentDatabase database, DocumentsOperationContext context, PatchRequest mainScript)
        : base(database, context, mainScript)
    {
    }

    public override ReturnRun CreateDocumentScriptRunner(bool debugMode, out SingleRun<JsHandleJint> documentScript)
    {
        return EtlTransformerHelper.CreateDocumentScriptRunnerJint(Database, _mainScript, debugMode, out documentScript);
    }

    protected override ScriptRunnerResult<JsHandleJint> CreateScriptRunnerResult(object obj)
    {
        return EtlTransformerHelper.GetScriptRunnerResultJint(obj, DocumentScript, EngineHandle);
    }
}