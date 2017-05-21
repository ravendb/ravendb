using System;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Json.Converters;
using Raven.Client.Server.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupStore
    {
        private readonly DocumentDatabase _database;
        private readonly ServerStore _serverStore;

        public PeriodicBackupStore(DocumentDatabase database, ServerStore serverStore)
        {
            _database = database;
            _serverStore = serverStore;
        }

        /*public async Task DeletePeriodicBackupStatus(long taskId)
        {
            TransactionOperationContext context;
            using (_serverStore.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var statusBlittable = _serverStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(_database.Name, taskId));
                return statusBlittable;
            }
        }*/

        public BlittableJsonReaderObject GetPeriodicBackupStatusAsBlittable(long taskId)
        {
            TransactionOperationContext context;
            using (_serverStore.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var statusBlittable = _serverStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(_database.Name, taskId));
                return statusBlittable;
            }
        }

        public PeriodicBackupStatus GetPeriodicBackupStatus(long taskId)
        {
            TransactionOperationContext context;
            using (_serverStore.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var statusBlittable = _serverStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(_database.Name, taskId));

                if (statusBlittable == null)
                    return null;

                var periodicBackupStatusJson = JsonDeserializationClient.PeriodicBackupStatus(statusBlittable);
                return periodicBackupStatusJson;
            }
        }
    }
}
