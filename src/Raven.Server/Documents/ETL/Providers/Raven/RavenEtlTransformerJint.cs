using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Raven;

public class RavenEtlTransformerJint : RavenEtlTransformerBase<JsHandleJint>
{
    public RavenEtlTransformerJint(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, ScriptInput script)
        : base(database, context, transformation, script)
    {
    }

    protected override ScriptRunnerResult<JsHandleJint> CreateScriptRunnerResult(object obj)
    {
        return EtlTransformerHelper.GetScriptRunnerResultJint(obj, DocumentScript, EngineHandle);
    }

    public override ReturnRun CreateBehaviorsScriptRunner(bool debugMode, out SingleRun<JsHandleJint> behaviorsScript)
    {
        var returnRun = Database.Scripts.GetScriptRunnerJint(_behaviorFunctions, readOnly: true, out behaviorsScript);
        if (behaviorsScript != null)
            behaviorsScript.DebugMode = debugMode;

        return returnRun;
    }

    public override RavenEtlScriptRun<JsHandleJint> CreateRavenEtlScriptRun(EtlStatsScope stats)
    {
        return new RavenEtlScriptRunJint(EngineHandle, stats);
    }

    public override ReturnRun CreateDocumentScriptRunner(bool debugMode, out SingleRun<JsHandleJint> documentScript)
    {
        return EtlTransformerHelper.CreateDocumentScriptRunnerJint(Database, _mainScript, debugMode, out documentScript);
    }
}
