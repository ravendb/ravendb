using System.Linq;
using Raven.Database.Config;
using System;
using System.IO;
using Voron;
using Voron.Impl.Backup;

namespace Raven.Database.Storage.Voron.Backup
{
    public class RestoreOperation : BaseRestoreOperation
    {
		public RestoreOperation(string backupLocation, InMemoryRavenConfiguration configuration, Action<string> operationOutputCallback)
            : base(backupLocation,configuration,operationOutputCallback)
		{
		}

        public override void Execute()
        {
			var logsPath = ValidateRestorePreconditionsAndReturnLogsPath(BackupMethods.Filename);

            try
            {
                CopyIndexes();
				CopyIndexDefinitions();

				var backupFilenamePath = BackupFilenamePath(BackupMethods.Filename);

				if (Directory.GetDirectories(backupLocation, "Inc*").Any() == false)
		            BackupMethods.Full.Restore(backupFilenamePath, configuration.DataDirectory, logsPath);
	            else
				{
                    using (var options = StorageEnvironmentOptions.ForPath(configuration.DataDirectory, journalPath: logsPath))
                    {
                        var backupPaths = Directory.GetDirectories(backupLocation, "Inc*")
                            .OrderBy(dir=>dir)
                            .Select(dir=> Path.Combine(dir,BackupMethods.Filename))
                            .ToList();
                        BackupMethods.Incremental.Restore(options,backupPaths);
                    }
				}

            }
            catch (Exception e)
            {
                LogFailureAndRethrow(e);
            }
        }
    }
}
