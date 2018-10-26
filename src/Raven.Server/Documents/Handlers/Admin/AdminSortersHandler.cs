using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents.Indexes.Sorting;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminSortersHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/sorters", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task Put()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var input = await context.ReadForMemoryAsync(RequestBodyStream(), "Sorters");
                if (input.TryGet("Sorters", out BlittableJsonReaderArray sorters) == false)
                    ThrowRequiredPropertyNameInRequest("Sorters");

                var command = new PutSortersCommand(Database.Name);
                foreach (var sorterToAdd in sorters)
                {
                    var sorterDefinition = JsonDeserializationServer.SorterDefinition((BlittableJsonReaderObject)sorterToAdd);
                    sorterDefinition.Name = sorterDefinition.Name?.Trim();

                    if (LoggingSource.AuditLog.IsInfoEnabled)
                    {
                        var clientCert = GetCurrentCertificate();

                        var auditLog = LoggingSource.AuditLog.GetLogger(Database.Name, "Audit");
                        auditLog.Info($"Sorter {sorterDefinition.Name} PUT by {clientCert?.Subject} {clientCert?.Thumbprint} with definition: {sorterToAdd}");
                    }

                    if (string.IsNullOrWhiteSpace(sorterDefinition.Name))
                        throw new ArgumentException("Sorter must have a 'Name' field");

                    if (string.IsNullOrWhiteSpace(sorterDefinition.Code))
                        throw new ArgumentException("Sorter must have a 'Code' field");

                    // check if sorter is compilable
                    SorterCompilationCache.AddSorter(sorterDefinition, Database.Name);

                    command.Sorters.Add(sorterDefinition);
                }

                var index = (await ServerStore.SendToLeaderAsync(command)).Index;

                await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
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

            var command = new DeleteSorterCommand(name, Database.Name);
            var index = (await ServerStore.SendToLeaderAsync(command)).Index;

            await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);

            NoContentStatus();
        }
    }
}
