using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL.Handlers.Processors;

internal sealed class SqlEtlHandlerProcessorForTestConnection<TOperationContext> : AbstractDatabaseHandlerProcessor<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    public SqlEtlHandlerProcessorForTestConnection([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler)
        : base(requestHandler)
    {
    }

    public override ValueTask ExecuteAsync() => new(SqlEtlTestConnectionHelper.ExecuteAsync(RequestHandler));
}
