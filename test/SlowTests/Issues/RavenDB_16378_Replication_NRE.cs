using System;
using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16378_Replication_NRE : ReplicationTestBase
    {
        public RavenDB_16378_Replication_NRE(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldNotThrowNreOnReplication()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "RavenDB-16378-NRE.ravendb-snapshot");

            using (var file = File.Create(fullBackupPath))
            {
                // this is a snapshot backup of database which is not completely "valid"
                // it was created a bit artificially by trying hard to break things when playing with revisions and output reduce results
                // for example some it includes some artificial documents which don't have @artificial flag (modified manually)
                // the point of using this snapshot here is that it reproduces the NRE problem (or assertion in AssertNoReferenceToThisPage when running in Debug)

                using (var stream = typeof(RavenDB_16378).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_16378.RavenDB-16401-NRE.ravendb-snapshot"))
                {
                    stream.CopyTo(file);
                }
            }

            using (var mainStore = GetDocumentStore())
            {
                var restoredDatabaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(mainStore, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = restoredDatabaseName
                }))
                using (var sourceOfRestoredDb = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => restoredDatabaseName,
                    CreateDatabase = false
                }))
                using (var destination = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_dst"
                }))
                {
                    using (var session = sourceOfRestoredDb.OpenSession())
                    {
                        session.Store(new { Foo = "marker"}, "marker");

                        session.SaveChanges();
                    }

                    await SetupReplicationAsync(sourceOfRestoredDb, destination);

                    WaitForDocument(destination, "marker");

                    var stats = destination.Maintenance.Send(new GetStatisticsOperation());

                    Assert.Equal(6, stats.CountOfDocuments);
                }
            }
        }
    }
}
