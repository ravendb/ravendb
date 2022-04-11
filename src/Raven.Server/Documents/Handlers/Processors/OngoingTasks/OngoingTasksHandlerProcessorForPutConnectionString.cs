using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class
        OngoingTasksHandlerProcessorForPutConnectionString<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        private readonly string _databaseName;

        public OngoingTasksHandlerProcessorForPutConnectionString([NotNull] TRequestHandler requestHandler,
            [NotNull] JsonContextPoolBase<TOperationContext> contextPool, [NotNull] string databaseName)
            : base(requestHandler, contextPool)
        {
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        }

        public override async ValueTask ExecuteAsync()
        {
            await DatabaseRequestHandler.DatabaseConfigurations(RequestHandler.ServerStore.PutConnectionString, "put-connection-string",
                RequestHandler.GetRaftRequestIdFromQuery(),
                _databaseName, RequestHandler);
        }
    }
}
