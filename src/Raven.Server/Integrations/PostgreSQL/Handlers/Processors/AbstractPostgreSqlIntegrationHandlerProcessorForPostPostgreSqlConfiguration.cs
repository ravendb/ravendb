using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal abstract class AbstractPostgreSqlIntegrationHandlerProcessorForPostPostgreSqlConfiguration<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject,TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractPostgreSqlIntegrationHandlerProcessorForPostPostgreSqlConfiguration([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override ValueTask AssertCanExecuteAsync(string databaseName)
    {
        AbstractPostgreSqlIntegrationHandlerProcessor<TRequestHandler, TOperationContext>.AssertCanUsePostgreSqlIntegration(RequestHandler);

        return base.AssertCanExecuteAsync(databaseName);
    }

    protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configuration, string raftRequestId)
    {
        return RequestHandler.ServerStore.ModifyPostgreSqlConfiguration(context, databaseName, configuration, raftRequestId);
    }
}
