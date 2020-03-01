using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Voron;
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
            string recoverDbName = $"RecoverDB_{Guid.NewGuid().ToString()}";
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

                var _ = store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord {DatabaseName = recoverDbName}));


                var recoveredDatabase = await GetDatabase(recoverDbName);
                using (var recovery = new Recovery(new VoronRecoveryConfiguration()
                {
                    LoggingMode = Sparrow.Logging.LogMode.None,
                    DataFileDirectory = dbPath,
                    PathToDataFile = Path.Combine(dbPath, "Raven.voron"),
                    LoggingOutputPath = Path.Combine(recoveryExportPath),
                    MasterKey = masterKey,
                    DisableCopyOnWriteMode = nullifyMasterKey,
                    RecoveredDatabase = recoveredDatabase
                }))
                {
                    recovery.Execute(TextWriter.Null, CancellationToken.None);
                }
            }

            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
            }))
            using (var __ = EnsureDatabaseDeletion(recoverDbName, store))
            {
                var currentStats = store.Maintenance.ForDatabase(recoverDbName).Send(new GetStatisticsOperation());

                Assert.Equal(databaseStatistics.CountOfAttachments, currentStats.CountOfAttachments);
                Assert.Equal(databaseStatistics.CountOfDocuments + 1, currentStats.CountOfDocuments); // + 1 as recovery adds RecoverLog document
            }
        }
    }
}
