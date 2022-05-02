using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForRemoveConnectionString<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler>
        where TRequestHandler : RequestHandler
    {

        protected AbstractOngoingTasksHandlerProcessorForRemoveConnectionString([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            var connectionStringName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("connectionString");
            var type = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            await RequestHandler.ServerStore.EnsureNotPassiveAsync();

            return await RequestHandler.ServerStore.RemoveConnectionString(databaseName, connectionStringName, type, raftRequestId);
        }
    }
}
