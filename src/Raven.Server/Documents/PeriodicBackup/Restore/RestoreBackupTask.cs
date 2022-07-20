using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    internal class RestoreBackupTask : AbstractRestoreBackupTask
    {
        public RestoreBackupTask(ServerStore serverStore, RestoreBackupConfigurationBase restoreConfiguration, IRestoreSource restoreSource, 
            List<string> filesToRestore, OperationCancelToken operationCancelToken) : base(serverStore, restoreConfiguration, restoreSource, filesToRestore, operationCancelToken)
        {
        }

        protected override async Task Restore(DocumentDatabase database, DocumentsOperationContext context)
        {
            await SmugglerRestore(database, context);
        }

        protected override async Task Initialize()
        {
            await base.Initialize();

            Result.SnapshotRestore.Skipped = true;
            Result.SnapshotRestore.Processed = true;

            Progress.Invoke(Result.Progress);

            RestoreSettings = new RestoreSettings
            {
                DatabaseRecord = GetDatabaseRecord()
            };

            if (ValidateResourceName)
            {
                DatabaseHelper.Validate(DatabaseName, RestoreSettings.DatabaseRecord, ServerStore.Configuration);
            }
        }

        protected virtual DatabaseRecord GetDatabaseRecord()
        {
            return new DatabaseRecord(DatabaseName)
            {
                // we only have a smuggler restore
                // use the encryption key to encrypt the database
                Encrypted = HasEncryptionKey
            };
        }
    }
}
