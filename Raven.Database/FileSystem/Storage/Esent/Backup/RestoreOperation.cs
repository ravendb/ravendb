//-----------------------------------------------------------------------
// <copyright file="RestoreOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;

using Microsoft.Isam.Esent.Interop;

using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Config;

using System.Linq;

namespace Raven.Database.FileSystem.Storage.Esent.Backup
{
    internal class RestoreOperation : BaseRestoreOperation
    {
        public RestoreOperation(FilesystemRestoreRequest restoreRequest, InMemoryRavenConfiguration configuration, Action<string> operationOutputCallback)
            : base(restoreRequest, configuration, operationOutputCallback)
        {
        }


        protected override bool IsValidBackup(string backupFilename)
        {
            return File.Exists(Path.Combine(backupLocation, backupFilename));
        }

        protected override void CheckBackupOwner()
        {
            Database.Storage.Esent.Backup.RestoreOperation.CheckBackupOwner(backupLocation, output);
        }

        public override void Execute()
        {
            ValidateRestorePreconditionsAndReturnLogsPath("RavenDB.Backup");
            
            Directory.CreateDirectory(Path.Combine(journalLocation, "logs"));
            Directory.CreateDirectory(Path.Combine(journalLocation, "temp"));
            Directory.CreateDirectory(Path.Combine(journalLocation, "system"));

            CombineIncrementalBackups();

            CopyIndexes();

            var dataFilePath = Path.Combine(databaseLocation, "Data.ravenfs");

            bool hideTerminationException = false;
            JET_INSTANCE instance;
            Raven.Storage.Esent.TransactionalStorage.CreateInstance(out instance, "restoring " + Guid.NewGuid());
            try
            {
                Configuration.Storage.Esent.JournalsStoragePath = journalLocation;
                new TransactionalStorageConfigurator(Configuration).ConfigureInstance(instance, databaseLocation);
                Api.JetRestoreInstance(instance, backupLocation, databaseLocation, RestoreStatusCallback);
                var fileThatGetsCreatedButDoesntSeemLikeItShould =
                    new FileInfo(
                        Path.Combine(
                            new DirectoryInfo(databaseLocation).Parent.FullName, new DirectoryInfo(databaseLocation).Name + "Data"));

                Raven.Storage.Esent.TransactionalStorage.DisableIndexChecking(instance);

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
                foreach (var file in Directory.GetFiles(directory, "RFS*.log"))
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

        private JET_err CompactStatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
        {
            output(string.Format("Esent Compact: {0} {1} {2}", snp, snt, data));
            Console.WriteLine("Esent Compact: {0} {1} {2}", snp, snt, data);
            return JET_err.Success;
        }
    }
}
