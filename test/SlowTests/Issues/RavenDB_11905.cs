using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Voron.Recovery;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11905 : RavenTestBase
    {
        [FactAndSkipWhen32BitsEnvironment]
        public async Task CanUseVoronRecoveryOnEmptyDatabase()
        {
            var dbPath = NewDataPath();
            var recoveryExportPath = NewDataPath();
            
            using (GetDocumentStore(new Options()
            {
                Path = dbPath
            }))
            {
                
            }

            using (var recovery = new Recovery(new VoronRecoveryConfiguration()
            {
                LoggingMode = Sparrow.Logging.LogMode.None,
                DataFileDirectory = dbPath,
                PathToDataFile = Path.Combine(dbPath, "Raven.voron"),
                OutputFileName = Path.Combine(recoveryExportPath, "recovery.ravendump"),
            }))
            {
                recovery.Execute(TextWriter.Null, CancellationToken.None);
            }
            
            using (var store = GetDocumentStore())
            {
                var op = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                {

                }, Path.Combine(recoveryExportPath, "recovery-2-Documents.ravendump"));

                op.WaitForCompletion(TimeSpan.FromMinutes(2));

                var databaseStatistics = store.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(0, databaseStatistics.CountOfAttachments);
                Assert.Equal(0, databaseStatistics.CountOfDocuments);
            }
        }
    }
}
