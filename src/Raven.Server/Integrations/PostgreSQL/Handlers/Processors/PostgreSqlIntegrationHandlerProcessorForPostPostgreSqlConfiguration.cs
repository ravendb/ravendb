using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal class PostgreSqlIntegrationHandlerProcessorForPostPostgreSqlConfiguration : AbstractPostgreSqlIntegrationHandlerProcessorForPostPostgreSqlConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{
    public PostgreSqlIntegrationHandlerProcessorForPostPostgreSqlConfiguration([NotNull] DatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }
}
