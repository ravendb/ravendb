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
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8451 : RavenTestBase
    {
        public RavenDB_8451(ITestOutputHelper output) : base(output)
        {
        }

        [Fact64Bit]
        public async Task CanRecoverEncryptedDatabase()
        {
            await CanRecoverEncryptedDatabaseInternal();
        }

        [Fact64Bit]
        public async Task RecoveryOfEncryptedDatabaseWithoutMasterKeyShouldThrow()
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await CanRecoverEncryptedDatabaseInternal(true));

        }
        public async Task CanRecoverEncryptedDatabaseInternal(bool nullifyMasterKey = false)
        {
            string dbName = SetupEncryptedDatabase(out var certificates, out var masterKey);
            
            if (nullifyMasterKey)
            {
                masterKey = null;
            }

            var dbPath = NewDataPath(prefix: Guid.NewGuid().ToString());
            var recoveryExportPath = NewDataPath(prefix: Guid.NewGuid().ToString());

            DatabaseStatistics databaseStatistics;

            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
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
                MasterKey = masterKey,
                DisableCopyOnWriteMode = nullifyMasterKey
            }))
            {
                recovery.Execute(TextWriter.Null, CancellationToken.None);
            }


            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
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
