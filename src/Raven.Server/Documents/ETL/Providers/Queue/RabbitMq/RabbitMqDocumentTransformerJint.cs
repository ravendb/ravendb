using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue.RabbitMq;

public class RabbitMqDocumentTransformerJint<T> : RabbitMqDocumentTransformerBase<T, JsHandleJint> where T : QueueItem
{
    public RabbitMqDocumentTransformerJint(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, QueueEtlConfiguration config)
        : base(transformation, database, context, config)
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
