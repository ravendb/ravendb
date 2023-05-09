using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForPutConnectionString<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private long _index;

        protected AbstractOngoingTasksHandlerProcessorForPutConnectionString([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            var res = await RequestHandler.ServerStore.PutConnectionString(context, RequestHandler.DatabaseName, configuration, raftRequestId);
            _index = res.Item1;
            return res;
        }

        protected override ValueTask OnAfterUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            RequestHandler.LogTaskToAudit(Web.RequestHandler.PutConnectionStringDebugTag, _index, configuration);
            return ValueTask.CompletedTask;
        }
    }
}
