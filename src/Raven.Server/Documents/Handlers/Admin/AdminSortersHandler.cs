using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries.Sorting;
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

                var command = new PutSortersCommand(Database.Name, GetRaftRequestIdFromQuery());
                foreach (var sorterToAdd in sorters)
                {
                    var sorterDefinition = JsonDeserializationServer.SorterDefinition((BlittableJsonReaderObject)sorterToAdd);
                    sorterDefinition.Name = sorterDefinition.Name?.Trim();

                    if (LoggingSource.AuditLog.IsInfoEnabled)
                    {
                        LogAuditFor(Database.Name, "PUT", $"Sorter '{sorterDefinition.Name}' with definition: {sorterToAdd}");
                    }

                    sorterDefinition.Validate();

                    // check if sorter is compilable
                    SorterCompiler.Compile(sorterDefinition.Name, sorterDefinition.Code);

                    command.Sorters.Add(sorterDefinition);
                }

                var index = (await ServerStore.SendToLeaderAsync(command)).Index;

                await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);

                NoContentStatus(HttpStatusCode.Created);
            }
        }

        [RavenAction("/databases/*/admin/sorters", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                LogAuditFor(Database.Name, "DELETE", $"Sorter '{name}'");
            }

            var command = new DeleteSorterCommand(name, Database.Name, GetRaftRequestIdFromQuery());
            var index = (await ServerStore.SendToLeaderAsync(command)).Index;

            await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);

            NoContentStatus();
        }
    }
}
