using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class
        OngoingTasksHandlerProcessorForPutConnectionString<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public OngoingTasksHandlerProcessorForPutConnectionString([NotNull] TRequestHandler requestHandler,
            [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
            : base(requestHandler, contextPool)
        {
        }

        private string GetDatabaseName()
        {
            return RequestHandler switch
            {
                ShardedDatabaseRequestHandler sharded => sharded.DatabaseContext.DatabaseName,
                DatabaseRequestHandler database => database.Database.Name,
                _ => null
            };
        }

        public override async ValueTask ExecuteAsync()
        {
            var databaseName = GetDatabaseName();
            await DatabaseRequestHandler.DatabaseConfigurations(RequestHandler.ServerStore.PutConnectionString, "put-connection-string",
                RequestHandler.GetRaftRequestIdFromQuery(),
                databaseName, RequestHandler);
        }
    }
}
