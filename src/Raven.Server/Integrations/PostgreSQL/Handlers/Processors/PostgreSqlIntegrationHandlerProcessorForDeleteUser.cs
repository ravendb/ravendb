using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal sealed class PostgreSqlIntegrationHandlerProcessorForDeleteUser : AbstractPostgreSqlIntegrationHandlerProcessorForDeleteUser<DatabaseRequestHandler, DocumentsOperationContext>
{
    public PostgreSqlIntegrationHandlerProcessorForDeleteUser([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
