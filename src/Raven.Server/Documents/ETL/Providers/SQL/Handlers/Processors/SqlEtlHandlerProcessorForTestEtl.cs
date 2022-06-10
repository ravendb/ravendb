using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL.Handlers.Processors
{
    internal class SqlEtlHandlerProcessorForTestEtl : AbstractEtlHandlerProcessorForTestEtl<DatabaseRequestHandler, DocumentsOperationContext, TestSqlEtlScript, SqlEtlConfiguration, SqlConnectionString>
    {
        public SqlEtlHandlerProcessorForTestEtl([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override TestSqlEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestSqlEtlScript(json);

        protected override IDisposable TestScript(DocumentsOperationContext context, TestSqlEtlScript testScript, out TestEtlScriptResult testResult)
        {
            return SqlEtl.TestScript(testScript, RequestHandler.Database, RequestHandler.ServerStore, context, out testResult);
        }

        protected override ValueTask HandleRemoteNodeAsync(DocumentsOperationContext context, TestSqlEtlScript testScript, BlittableJsonReaderObject testScriptJson) => throw new NotSupportedException();
    }
}
