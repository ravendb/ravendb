using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Voron.Recovery;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8451: RavenTestBase
    {
        [Fact64Bit]
        public async Task CanRecoverEncryptedDatabase()
        {
            string dbName = SetupEncryptedDatabase(out X509Certificate2 adminCert, out var masterKey);

            var dbPath = NewDataPath(prefix: Guid.NewGuid().ToString());
            var recoveryExportPath = NewDataPath(prefix: Guid.NewGuid().ToString());

            DatabaseStatistics databaseStatistics;

            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                Path = dbPath
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                databaseStatistics = store.Maintenance.Send(new GetStatisticsOperation());
            }
           
            using (var recovery = new Recovery(new VoronRecoveryConfiguration()
            {
                LoggingMode = Sparrow.Logging.LogMode.None,
                DataFileDirectory = dbPath,
                PathToDataFile = Path.Combine(dbPath, "Raven.voron"),
                OutputFileName = Path.Combine(recoveryExportPath, "recovery.ravendump"),
                MasterKey = masterKey
            }))
            {
                recovery.Execute(TextWriter.Null, CancellationToken.None);
            }
            

            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            }))
            {
                var op = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                {

                }, Path.Combine(recoveryExportPath, "recovery-2-Documents.ravendump"));

                op.WaitForCompletion(TimeSpan.FromMinutes(2));

                var currentStats = store.Maintenance.Send(new GetStatisticsOperation());

                // + 1 as recovery adds some artificial items
                Assert.Equal(databaseStatistics.CountOfAttachments + 1, currentStats.CountOfAttachments);
                Assert.Equal(databaseStatistics.CountOfDocuments + 1, currentStats.CountOfDocuments);
            }
        }
    }
}
