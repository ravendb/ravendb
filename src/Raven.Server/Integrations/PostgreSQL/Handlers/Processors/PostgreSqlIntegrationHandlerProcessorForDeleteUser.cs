using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal class PostgreSqlIntegrationHandlerProcessorForDeleteUser : AbstractPostgreSqlIntegrationHandlerProcessorForDeleteUser<DatabaseRequestHandler>
{
    public PostgreSqlIntegrationHandlerProcessorForDeleteUser([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.Database.Name;

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.Database.ServerStore.Engine.OperationTimeout);
    }
}
