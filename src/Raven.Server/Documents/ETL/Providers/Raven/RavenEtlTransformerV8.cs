using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Raven;

public class RavenEtlTransformerV8 : RavenEtlTransformerBase<JsHandleV8>
{
    public RavenEtlTransformerV8(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, ScriptInput script)
        : base(database, context, transformation, script)
    {
    }

    protected override ScriptRunnerResult<JsHandleV8> CreateScriptRunnerResult(object obj)
    {
        return EtlTransformerHelper.GetScriptRunnerResultV8(obj, DocumentScript, EngineHandle);
    }

    public override ReturnRun CreateBehaviorsScriptRunner(bool debugMode, out SingleRun<JsHandleV8> behaviorsScript)
    {
        var returnRun = Database.Scripts.GetScriptRunnerV8(_behaviorFunctions, readOnly: true, out behaviorsScript);
        if (behaviorsScript != null)
            behaviorsScript.DebugMode = debugMode;

        return returnRun;
    }

    public override RavenEtlScriptRun<JsHandleV8> CreateRavenEtlScriptRun(EtlStatsScope stats)
    {
        return new RavenEtlScriptRunV8(EngineHandle, stats);
    }

    public override ReturnRun CreateDocumentScriptRunner(bool debugMode, out SingleRun<JsHandleV8> documentScript)
    {
        return EtlTransformerHelper.CreateDocumentScriptRunnerV8(Database, _mainScript, debugMode, out documentScript);
    }
}
