using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Voron.Recovery;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11904 : RavenTestBase
    {
        public RavenDB_11904(ITestOutputHelper output) : base(output)
        {
        }

        [Fact64Bit]
        public async Task CanLoadDatabaseAfterUsingVoronRecoveryOnItWithCopyOnWriteMode()
        {
            var dbPath = NewDataPath(prefix: Guid.NewGuid().ToString());
            var recoveryExportPath = NewDataPath(prefix: Guid.NewGuid().ToString());

            DatabaseStatistics databaseStatistics;

            string recoverDbName = $"RecoverDB_{Guid.NewGuid().ToString()}";
            FileInfo[] journals = null;
            // create db with sample data
            using (var store = GetDocumentStore(new Options()
            {
                Path = dbPath
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());
                databaseStatistics = store.Maintenance.Send(new GetStatisticsOperation());

                var _ = store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord {DatabaseName = recoverDbName}));

                journals = new DirectoryInfo(Path.Combine(dbPath, "Journals")).GetFiles();

                // run recovery
                var recoveredDatabase = await GetDatabase(recoverDbName);
                using (var recovery = new Recovery(new VoronRecoveryConfiguration()
                {
                    LoggingMode = Sparrow.Logging.LogMode.None,
                    DataFileDirectory = dbPath,
                    PathToDataFile = Path.Combine(dbPath, "Raven.voron"),
                    LoggingOutputPath = Path.Combine(recoveryExportPath),
                    RecoveredDatabase = recoveredDatabase
                }))
                {
                    recovery.Execute(TextWriter.Null, CancellationToken.None);
                }
            }

            // make sure no journal file was lost during the process - by default we use copy on write mode
            var journalsAfterRecovery = new DirectoryInfo(Path.Combine(dbPath, "Journals")).GetFiles();

            Assert.Equal(journals.Length, journalsAfterRecovery.Length);
            Assert.True(journals.All(x => journalsAfterRecovery.Any(y => y.Name == x.Name)));

            // let's open the database
            using (var store = GetDocumentStore(new Options()
            {
                Path = dbPath
            }))
            {
                var currentStats = store.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(databaseStatistics.CountOfAttachments, currentStats.CountOfAttachments);
                Assert.Equal(databaseStatistics.CountOfDocuments, currentStats.CountOfDocuments);
            }

            // let's import the recovery files

            using (var store = GetDocumentStore())
            using (var __ = EnsureDatabaseDeletion(recoverDbName, store))
            {
                var currentStats = store.Maintenance.ForDatabase(recoverDbName).Send(new GetStatisticsOperation());

                Assert.Equal(databaseStatistics.CountOfAttachments, currentStats.CountOfAttachments);
                Assert.Equal(databaseStatistics.CountOfDocuments + 1, currentStats.CountOfDocuments);  // + 1 as recovery adds RecoverLog document
            }
        }
    }
}
