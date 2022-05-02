using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class
        OngoingTasksHandlerProcessorForPutConnectionString<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public OngoingTasksHandlerProcessorForPutConnectionString([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            await RequestHandler.DatabaseConfigurations(RequestHandler.ServerStore.PutConnectionString, 
                "put-connection-string",
                RequestHandler.GetRaftRequestIdFromQuery(),
                _databaseName, 
                waitForIndex: RequestHandler.WaitForIndexToBeAppliedAsync);
        }
    }
}
