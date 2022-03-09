using System;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractConfigurationHandlerProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractConfigurationHandlerProcessor(TRequestHandler requestHandler, JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        { }

        protected abstract ValueTask WaitForIndexNotificationAsync(long index);

        protected async Task UpdateDatabaseRecord(TransactionOperationContext context, Action<DatabaseRecord, long> action, string raftRequestId, string databaseName)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            long raftCommandIndex;
            using (context.OpenReadTransaction())
            {
                var record = RequestHandler.ServerStore.Cluster.ReadDatabase(context, databaseName, out long index);

                action(record, index);

                var result = await RequestHandler.ServerStore.WriteDatabaseRecordAsync(databaseName, record, index, raftRequestId);
                raftCommandIndex = result.Index;
            }

            await WaitForIndexNotificationAsync(raftCommandIndex);
        }
    }
}
