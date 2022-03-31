using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin.Processors.Sorters;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.Sorters;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminSortersHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/sorters", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task Put()
        {
            using (var processor = new AdminSortersHandlerProcessorForPut(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/sorters", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                var clientCert = GetCurrentCertificate();

                var auditLog = LoggingSource.AuditLog.GetLogger(Database.Name, "Audit");
                auditLog.Info($"Sorter {name} DELETE by {clientCert?.Subject} {clientCert?.Thumbprint}");
            }

            var command = new DeleteSorterCommand(name, Database.Name, GetRaftRequestIdFromQuery());
            var index = (await ServerStore.SendToLeaderAsync(command)).Index;

            await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);

            NoContentStatus();
        }
    }
}
