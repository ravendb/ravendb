using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Config;

using System;
using System.IO;

using Voron;
using Voron.Impl.Backup;

namespace Raven.Database.Server.RavenFS.Storage.Voron.Backup
{
    public class RestoreOperation : BaseRestoreOperation
    {
		public RestoreOperation(FilesystemRestoreRequest restoreRequest, InMemoryRavenConfiguration configuration, Action<string> operationOutputCallback)
            : base(restoreRequest, configuration, operationOutputCallback)
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

				var backupFilenamePath = BackupFilenamePath(BackupMethods.Filename);

				if (Directory.GetDirectories(backupLocation, "Inc*").Any() == false)
		            BackupMethods.Full.Restore(backupFilenamePath, Configuration.DataDirectory, journalLocation);
	            else
				{
                    using (var options = StorageEnvironmentOptions.ForPath(Configuration.DataDirectory, journalPath: journalLocation))
                    {
                        var backupPaths = Directory.GetDirectories(backupLocation, "Inc*")
                            .OrderBy(dir=>dir)
                            .Select(dir=> Path.Combine(dir,BackupMethods.Filename))
                            .ToList();
                        BackupMethods.Incremental.Restore(options, backupPaths);
                    }
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
