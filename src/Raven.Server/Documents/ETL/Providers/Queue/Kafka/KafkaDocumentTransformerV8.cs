using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue.Kafka;

public class KafkaDocumentTransformerV8<T> : KafkaDocumentTransformerBase<T, JsHandleV8> where T : QueueItem
{
    public KafkaDocumentTransformerV8(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, QueueEtlConfiguration config)
        : base(transformation, database, context, config)
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
