using System.Collections.Generic;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL;

public abstract class RavenEtlTransformerBase<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation, T> : EtlTransformer<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation, T>
    where TExtracted : ExtractedItem
    where TStatsScope : AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation>
    where TEtlPerformanceOperation : EtlPerformanceOperation
    where T : struct, IJsHandle<T>
{
    protected readonly PatchRequest _behaviorFunctions;
    protected SingleRun<T> BehaviorsScript;
    public IJsEngineHandle<T> BehaviorsEngineHandle;
    protected ReturnRun _behaviorFunctionsRun;


    protected RavenEtlTransformerBase(DocumentDatabase database, DocumentsOperationContext context,
        PatchRequest mainScript, PatchRequest behaviorFunctions) : base(database, context, mainScript)
    {
        _behaviorFunctions = behaviorFunctions;
    }
    public abstract ReturnRun CreateBehaviorsScriptRunner(bool debugMode, out SingleRun<T> behaviorsScript);

    public override void Initialize(bool debugMode)
    {
        base.Initialize(debugMode);
        // initialize stuff for raven etl
        if (_behaviorFunctions != null)
            _behaviorFunctionsRun = CreateBehaviorsScriptRunner(debugMode, out BehaviorsScript);

        BehaviorsEngineHandle = BehaviorsScript?.EngineHandle;
    }

    public override List<string> GetDebugOutput()
    {
        var outputs = base.GetDebugOutput();
        if (BehaviorsScript?.DebugOutput != null)
            outputs.AddRange(BehaviorsScript.DebugOutput);
        return outputs;
    }

    public override void Dispose()
    {
        base.Dispose();

        using (_behaviorFunctionsRun)
        {

        }
    }
}