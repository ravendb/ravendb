using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using System;
using System.IO;
using Voron;
using Voron.Impl.Backup;

namespace Raven.Database.Storage.Voron.Backup
{
    internal class RestoreOperation : BaseRestoreOperation
    {
        public RestoreOperation(DatabaseRestoreRequest restoreRequest, InMemoryRavenConfiguration configuration, InMemoryRavenConfiguration globalConfiguration, Action<string> operationOutputCallback)
            : base(restoreRequest, configuration, globalConfiguration, operationOutputCallback)
        {
        }

        protected override bool IsValidBackup(string backupFilename)
        {
            return File.Exists(BackupFilenamePath(backupFilename));
        }


        public override void Execute()
        {
            ValidateRestorePreconditionsAndReturnLogsPath(BackupMethods.Filename);

            try
            {
                CopyIndexes();
                CopyIndexDefinitions();

                //if we have full & incremental in the same folder, do full restore first
                var fullBackupFilename = Path.Combine(backupLocation, "RavenDB.Voron.Backup");
                if (File.Exists(fullBackupFilename))
                {
                    BackupMethods.Full.Restore(fullBackupFilename, Configuration.DataDirectory, journalLocation);
                }

                using (var options = StorageEnvironmentOptions.ForPath(Configuration.DataDirectory, journalPath: journalLocation))
                {
                    var backupPaths = Directory.GetDirectories(backupLocation, "Inc*")
                        .OrderBy(dir => dir)
                        .Select(dir => Path.Combine(dir, BackupMethods.Filename))
                        .ToList();
                    BackupMethods.Incremental.Restore(options, backupPaths);
                }
            }
            catch (Exception e)
            {
                output("Restore Operation: Failure! Could not restore database!");
                output(e.ToString());
                log.WarnException("Could not complete restore", e);
                throw;
            }
        }

    }
}
