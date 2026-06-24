using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Snowflake.Handlers.Processors;

internal sealed class SnowflakeEtlHandlerProcessorForTestConnection<TOperationContext>([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler)
    : AbstractDatabaseHandlerProcessor<TOperationContext>(requestHandler)
    where TOperationContext : JsonOperationContext
{
    public override ValueTask ExecuteAsync() => new(SnowflakeEtlTestConnectionHelper.ExecuteAsync(RequestHandler));
}
