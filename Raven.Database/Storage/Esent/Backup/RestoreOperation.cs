//-----------------------------------------------------------------------
// <copyright file="RestoreOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Data;
using System.Linq;
using System.Security.Principal;
using Raven.Storage.Esent;

namespace Raven.Database.Storage.Esent.Backup
{
    internal class RestoreOperation : BaseRestoreOperation
    {
        public RestoreOperation(DatabaseRestoreRequest restoreRequest, InMemoryRavenConfiguration configuration, InMemoryRavenConfiguration globalConfiguration, Action<string> operationOutputCallback)
            : base(restoreRequest, configuration, globalConfiguration, operationOutputCallback)
        {
        }


        protected override bool IsValidBackup(string backupFilename)
        {
            return File.Exists(Path.Combine(backupLocation, backupFilename));
        }

        protected override void CheckBackupOwner()
        {
            CheckBackupOwner(backupLocation, output);
        }

        public static void CheckBackupOwner(string backupLocation, Action<string> output)
        {
            try
            {
                var dirAccess = Directory.GetAccessControl(backupLocation);
                var dirOwner = dirAccess.GetOwner(typeof(SecurityIdentifier));
                var currentUser = WindowsIdentity.GetCurrent().User;

                if (currentUser != null && currentUser != dirOwner)
                {
                    output($"WARNING: Current user '{currentUser.Translate(typeof(NTAccount))}' isn't an owner of the backup location ({backupLocation}, " +
                           $"current owner: '{dirOwner.Translate(typeof(NTAccount))}'). Restoring Esent backup might require user running RavenDB to be an owner of the backup files.");
                }
            }
            catch (Exception e)
            {
                output($"WARNING: Could not verify the owner of the backup location. Exception message: {e.Message}");
            }
        }

        public override void Execute()
        {
            ValidateRestorePreconditionsAndReturnLogsPath("RavenDB.Backup");
            
            Directory.CreateDirectory(Path.Combine(journalLocation, "logs"));
            Directory.CreateDirectory(Path.Combine(journalLocation, "temp"));
            Directory.CreateDirectory(Path.Combine(journalLocation, "system"));

            CombineIncrementalBackups();

            CopyIndexDefinitions();

            CopyIndexes();

            var dataFilePath = Path.Combine(databaseLocation, "Data");

            bool hideTerminationException = false;
            JET_INSTANCE instance;
            TransactionalStorage.CreateInstance(out instance, "restoring " + Guid.NewGuid());
            try
            {
                Configuration.Storage.Esent.JournalsStoragePath = journalLocation;
                new TransactionalStorageConfigurator(Configuration, null).ConfigureInstance(instance, databaseLocation);
                Api.JetRestoreInstance(instance, backupLocation, databaseLocation, RestoreStatusCallback);
                var fileName = Path.Combine(new DirectoryInfo(databaseLocation).Parent.FullName, new DirectoryInfo(databaseLocation).Name, "Data");
                var fileThatGetsCreatedButDoesntSeemLikeItShould = new FileInfo(fileName);

                TransactionalStorage.DisableIndexChecking(instance);

                if (fileThatGetsCreatedButDoesntSeemLikeItShould.Exists)
                {
                    fileThatGetsCreatedButDoesntSeemLikeItShould.MoveTo(dataFilePath);
                }

                if (_restoreRequest.Defrag)
                {
                    output("Esent Restore: Begin Database Compaction");
                    TransactionalStorage.Compact(Configuration, CompactStatusCallback);
                    output("Esent Restore: Database Compaction Completed");
                }
            }
            catch(Exception e)
            {
                output("Esent Restore: Failure! Could not restore database!");
                output(e.ToString());
                log.WarnException("Could not complete restore", e);
                hideTerminationException = true;
                throw;
            }
            finally
            {
                try
                {
                    Api.JetTerm(instance);
                }
                catch (Exception)
                {
                    if (hideTerminationException == false)
                        throw;
                }
            }
        }

        private void CombineIncrementalBackups()
        {
            var directories = Directory.GetDirectories(backupLocation, "Inc*")
                .OrderBy(dir => dir)
                .ToList();

            foreach (var directory in directories)
            {
                foreach (var file in Directory.GetFiles(directory, "RVN*.log"))
                {
                    var justFile = Path.GetFileName(file);
                    output(string.Format("Copying incremental log : {0}", justFile));
                    File.Copy(file, Path.Combine(backupLocation, "new", justFile), true);
                }
            }
        }

        private JET_err RestoreStatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
        {
            output(string.Format("Esent Restore: {0} {1} {2}", snp, snt, data));
            Console.WriteLine("Esent Restore: {0} {1} {2}", snp, snt, data);

            return JET_err.Success;
        }

        private DateTime lastCompactionProgressStatusUpdate;

        private JET_err CompactStatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
        {
            Console.WriteLine("Esent Compact: {0} {1} {2}", snp, snt, data);

            if (snt == JET_SNT.Progress)
            {
                if(SystemTime.UtcNow - lastCompactionProgressStatusUpdate < TimeSpan.FromMilliseconds(100))
                    return JET_err.Success;

                lastCompactionProgressStatusUpdate = SystemTime.UtcNow;
            }

            output(string.Format("Esent Compact: {0} {1} {2}", snp, snt, data));
            
            return JET_err.Success;
        }
    }
}
