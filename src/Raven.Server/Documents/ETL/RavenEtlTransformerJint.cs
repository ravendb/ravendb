using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL;

public abstract class RavenEtlTransformerJint<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation> : RavenEtlTransformerBase<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation, JsHandleJint>
    where TExtracted : ExtractedItem
    where TStatsScope : AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation>
    where TEtlPerformanceOperation : EtlPerformanceOperation
{

    protected RavenEtlTransformerJint(DocumentDatabase database, DocumentsOperationContext context, PatchRequest mainScript, PatchRequest behaviorFunctions)
        : base(database, context, mainScript, behaviorFunctions)
    {
    }

    protected override ScriptRunnerResult<JsHandleJint> CreateScriptRunnerResult(object obj)
    {
        return EtlTransformerHelper.GetScriptRunnerResultJint(obj, DocumentScript, EngineHandle);
    }
}