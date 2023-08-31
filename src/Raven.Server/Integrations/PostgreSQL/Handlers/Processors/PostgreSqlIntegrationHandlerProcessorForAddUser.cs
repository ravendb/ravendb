using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal sealed class PostgreSqlIntegrationHandlerProcessorForAddUser : AbstractPostgreSqlIntegrationHandlerProcessorForAddUser<DatabaseRequestHandler, DocumentsOperationContext>
{
    public PostgreSqlIntegrationHandlerProcessorForAddUser([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
