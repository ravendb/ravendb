using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public class ElasticSearchDocumentTransformerJint : ElasticSearchDocumentTransformerBase<JsHandleJint>
    {
        public ElasticSearchDocumentTransformerJint(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, ElasticSearchEtlConfiguration config)
            : base(transformation,database, context, config)
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
}
