using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.ETL.Providers.SQL.Test;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.SQL.Handlers.Processors
{
    internal class SqlEtlHandlerProcessorForTestSqlEtl : AbstractSqlEtlHandlerProcessorForTestSqlEtl<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SqlEtlHandlerProcessorForTestSqlEtl([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetAndWriteSqlEtlScriptTestResultAsync(DocumentsOperationContext context, BlittableJsonReaderObject sqlScript)
        {
            var testScript = JsonDeserializationServer.TestSqlEtlScript(sqlScript);
            
            using (SqlEtl.TestScript(testScript, RequestHandler.Database, ServerStore, context, out var testResult))
            {
                var result = (SqlEtlTestScriptResult)testResult;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(result);
                    writer.WriteObject(context.ReadObject(djv, "et/sql/test"));
                }
            }
        }
    }
}
