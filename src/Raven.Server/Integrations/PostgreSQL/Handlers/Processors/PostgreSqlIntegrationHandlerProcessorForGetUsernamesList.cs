using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal class PostgreSqlIntegrationHandlerProcessorForGetUsernamesList : AbstractPostgreSqlIntegrationHandlerProcessorForGetUsernamesList<DatabaseRequestHandler, DocumentsOperationContext>
{
    public PostgreSqlIntegrationHandlerProcessorForGetUsernamesList([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }
}
