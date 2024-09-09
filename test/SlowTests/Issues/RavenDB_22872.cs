using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22872 : ReplicationTestBase
    {
        public RavenDB_22872(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Attachments | RavenTestCategory.Replication)]
        public async Task ProperCalculationOfTombstoneConflictedId()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var src = GetDocumentStore())
            using (var dst = GetDocumentStore())
            {
                await SetupReplicationAsync(src, dst);
                await EnsureReplicatingAsync(src, dst);

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, dst);
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);

                var backupStatus = dst.Maintenance.Send(operation);
                var backupOperationId = backupStatus.Status.LastOperationId;

                var backupOperation = dst.Maintenance.Send(new GetOperationStateOperation(backupOperationId.Value));
                Assert.Equal(OperationStatus.Completed, backupOperation.Status);

                var backupResult = backupOperation.Result as BackupResult;
                Assert.Equal(1, backupResult.Documents.ReadCount);

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = src.OpenAsyncSession())
                using (var ms = new MemoryStream(new byte[]{1,2,3,4}))
                {
                    session.Advanced.Attachments.Store("users/1", "attachment", ms, "text/csv");
                    await session.SaveChangesAsync();
                }

                using (var session = src.OpenAsyncSession())
                {
                    session.Advanced.Attachments.Delete("users/1", "attachment");
                    await session.SaveChangesAsync();
                }

                using (var session = src.OpenAsyncSession())
                {
                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }

                await EnsureReplicatingAsync(src, dst);

                var lastEtag = dst.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, dst, isFullBackup: false, expectedEtag: lastEtag);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                var backupLocation = Directory.GetDirectories(backupPath).First();

                using (ReadOnly(backupLocation))
                using (Backup.RestoreDatabase(dst, new RestoreBackupConfiguration
                {
                    BackupLocation = backupLocation,
                    DatabaseName = databaseName
                }))
                {
                   // making sure we can finish the restore
                }
            }
        }

        private static IDisposable ReadOnly(string path)
        {
            var files = Directory.GetFiles(path);
            var attributes = new FileInfo(files[0]).Attributes;
            foreach (string file in files)
            {
                File.SetAttributes(file, FileAttributes.ReadOnly);
            }

            return new DisposableAction(() =>
            {
                foreach (string file in files)
                {
                    File.SetAttributes(file, attributes);
                }
            });
        }
    }
}
