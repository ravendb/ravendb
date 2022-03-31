using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Indexes.Sorting;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Sorters;

internal abstract class AbstractAdminSortersHandlerProcessorForPut<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractAdminSortersHandlerProcessorForPut([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract string GetDatabaseName();

    protected abstract ValueTask WaitForIndexNotificationAsync(long index);

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var input = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "Sorters");
            if (input.TryGet("Sorters", out BlittableJsonReaderArray sorters) == false)
                Web.RequestHandler.ThrowRequiredPropertyNameInRequest("Sorters");

            var databaseName = GetDatabaseName();
            var command = new PutSortersCommand(databaseName, RequestHandler.GetRaftRequestIdFromQuery());
            foreach (var sorterToAdd in sorters)
            {
                var sorterDefinition = JsonDeserializationServer.SorterDefinition((BlittableJsonReaderObject)sorterToAdd);
                sorterDefinition.Name = sorterDefinition.Name?.Trim();

                if (LoggingSource.AuditLog.IsInfoEnabled)
                {
                    var clientCert = RequestHandler.GetCurrentCertificate();

                    var auditLog = LoggingSource.AuditLog.GetLogger(databaseName, "Audit");
                    auditLog.Info($"Sorter {sorterDefinition.Name} PUT by {clientCert?.Subject} {clientCert?.Thumbprint} with definition: {sorterToAdd}");
                }

                sorterDefinition.Validate();

                // check if sorter is compilable
                SorterCompiler.Compile(sorterDefinition.Name, sorterDefinition.Code);

                command.Sorters.Add(sorterDefinition);
            }

            var index = (await RequestHandler.ServerStore.SendToLeaderAsync(command)).Index;

            await WaitForIndexNotificationAsync(index);

            RequestHandler.NoContentStatus(HttpStatusCode.Created);
        }
    }
}
