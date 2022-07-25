using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    internal class RestoreBackupTask : AbstractRestoreBackupTask
    {
        public RestoreBackupTask(ServerStore serverStore, RestoreBackupConfigurationBase restoreConfiguration, IRestoreSource restoreSource, 
            List<string> filesToRestore, OperationCancelToken operationCancelToken) : base(serverStore, restoreConfiguration, restoreSource, filesToRestore, operationCancelToken)
        {
        }

        protected override async Task Restore()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await SmugglerRestore(Database, context);
            }
        }

        protected override async Task Initialize()
        {
            await base.Initialize();

            Result.SnapshotRestore.Skipped = true;
            Result.SnapshotRestore.Processed = true;

            Progress.Invoke(Result.Progress);

            CreateRestoreSettings();
        }
    }
}
