using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Indexes.Sorting;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands.Sorters;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Sorters;

internal abstract class AbstractAdminSortersHandlerProcessorForPut<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminSortersHandlerProcessorForPut([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var input = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "Sorters");
            if (input.TryGet("Sorters", out BlittableJsonReaderArray sorters) == false)
                Web.RequestHandler.ThrowRequiredPropertyNameInRequest("Sorters");

            var databaseName = RequestHandler.DatabaseName;
            var command = new PutSortersCommand(databaseName, RequestHandler.GetRaftRequestIdFromQuery());
            foreach (var sorterToAdd in sorters)
            {
                var sorterDefinition = JsonDeserializationServer.SorterDefinition((BlittableJsonReaderObject)sorterToAdd);
                sorterDefinition.Name = sorterDefinition.Name?.Trim();

                if (LoggingSource.AuditLog.IsInfoEnabled)
                {
                    RequestHandler.LogAuditFor(databaseName, "PUT", $"Sorter '{sorterDefinition.Name}' with definition: {sorterToAdd}");
                }

                sorterDefinition.Validate();

                // check if sorter is compilable
                SorterCompiler.Compile(sorterDefinition.Name, sorterDefinition.Code);

                command.Sorters.Add(sorterDefinition);
            }

            var index = (await RequestHandler.ServerStore.SendToLeaderAsync(command)).Index;

            await RequestHandler.WaitForIndexNotificationAsync(index);

            RequestHandler.NoContentStatus(HttpStatusCode.Created);
        }
    }
}
