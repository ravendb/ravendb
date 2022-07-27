using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL;

public abstract class RavenEtlTransformerV8<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation> : RavenEtlTransformerBase<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation, JsHandleV8>
    where TExtracted : ExtractedItem
    where TStatsScope : AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation>
    where TEtlPerformanceOperation : EtlPerformanceOperation
{
    protected RavenEtlTransformerV8(DocumentDatabase database, DocumentsOperationContext context, PatchRequest mainScript, PatchRequest behaviorFunctions)
        : base(database, context, mainScript, behaviorFunctions)
    {
    }

    protected override ScriptRunnerResult<JsHandleV8> CreateScriptRunnerResult(object obj)
    {
        return EtlTransformerHelper.GetScriptRunnerResultV8(obj, DocumentScript, EngineHandle);
    }
}