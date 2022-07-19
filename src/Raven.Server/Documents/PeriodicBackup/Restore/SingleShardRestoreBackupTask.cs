using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    internal class SingleShardRestoreBackupTask : RestoreBackupTask
    {
        private readonly string _nonShardedDbName;
        public SingleShardRestoreBackupTask(ServerStore serverStore, RestoreBackupConfigurationBase restoreConfiguration, List<string> filesToRestore, 
            IRestoreSource restoreSource, OperationCancelToken operationCancelToken) : base(serverStore, restoreConfiguration, restoreSource, filesToRestore, operationCancelToken)
        {
            ValidateResourceName = false;
            ChangeDatabaseStateAfterRestore = false;
            HasEncryptionKey = false;

            _nonShardedDbName = ShardHelper.ToDatabaseName(DatabaseName);
        }

        protected override RavenConfiguration CreateDatabaseConfiguration()
        {
            return DatabasesLandlord.CreateDatabaseConfiguration(ServerStore, DatabaseName, RestoreSettings.DatabaseRecord.Settings);
        }

        protected override DatabaseRecord GetDatabaseRecord()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                var databaseRecord = ServerStore.Cluster.ReadDatabase(serverContext, _nonShardedDbName);
                Debug.Assert(databaseRecord != null);

                return databaseRecord;
            }
        }

        protected override Task OnBeforeRestore(DocumentDatabase database)
        {
            return Task.CompletedTask;
        }

        protected override ClusterTopology GetClusterTopology()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return ServerStore.GetClusterTopology(context);
            }
        }

        protected override Task<long> SaveDatabaseRecordAsync(string databaseName, DatabaseRecord databaseRecord, Dictionary<string, BlittableJsonReaderObject> databaseValues, RestoreResult restoreResult, Action<IOperationProgress> onProgress)
        {
            return base.SaveDatabaseRecordAsync(_nonShardedDbName, databaseRecord, databaseValues, restoreResult, onProgress);
        }
    }
}
