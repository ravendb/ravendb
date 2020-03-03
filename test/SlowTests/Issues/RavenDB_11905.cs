using System;
using System.IO;
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
    public class RavenDB_11905 : RavenTestBase
    {
        public RavenDB_11905(ITestOutputHelper output) : base(output)
        {
        }

        [Fact64Bit]
        public async Task CanUseVoronRecoveryOnEmptyDatabase()
        {
            var dbPath = NewDataPath();
            var recoveryExportPath = NewDataPath();
            string recoverDbName = $"RecoverDB_{Guid.NewGuid().ToString()}";

            using (var store = GetDocumentStore(new Options()
            {
                Path = dbPath
            }))
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord {DatabaseName = recoverDbName}));
            }

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
            
            using (var store = GetDocumentStore())
            using (EnsureDatabaseDeletion(recoverDbName, store))
            {
                var databaseStatistics = store.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(0, databaseStatistics.CountOfAttachments);
                Assert.Equal(0, databaseStatistics.CountOfDocuments);
            }
        }
    }
}
