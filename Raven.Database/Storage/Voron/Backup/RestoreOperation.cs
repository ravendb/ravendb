using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Extensions;

namespace Raven.Database.Storage.Voron.Backup
{
    public class RestoreOperation : BaseRestoreOperation
    {
        private const string VORON_BACKUP_FILENAME = "RavenDB.Voron.Backup";
        private const string VORON_DATABASE_FILENAME = "Raven.voron";
        
        

        public RestoreOperation(string backupLocation,InMemoryRavenConfiguration configuration, Action<string> operationOutputCallback)
            : base(backupLocation,configuration,operationOutputCallback)
        {
        }

        public void Execute()
        {
            ValidateRestorePreconditions(VORON_BACKUP_FILENAME);

            try
            {
                CopyIndexes();

                var backupFile = new FileInfo(BackupFilenamePath(VORON_BACKUP_FILENAME));

                backupFile.CopyTo(Path.Combine(configuration.DataDirectory, VORON_DATABASE_FILENAME), false);
            }
            catch (Exception e)
            {
                LogFailureAndRethrow(e);
            }
        }
    }
}
