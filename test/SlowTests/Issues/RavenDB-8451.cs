using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
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

        [Fact64Bit(Skip = "RavenDB-13765")]
        public async Task CanRecoverEncryptedDatabase()
        {
            await CanRecoverEncryptedDatabaseInternal();
        }

        [Fact64Bit(Skip = "RavenDB-13765")]
        public async Task CanRecoverEncryptedDatabase_Compressed()
        {
            await CanRecoverEncryptedDatabaseInternal(compressDocuments: true);
        }

        [Fact64Bit(Skip = "RavenDB-13765")]
        public async Task RecoveryOfEncryptedDatabaseWithoutMasterKeyShouldThrow()
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await CanRecoverEncryptedDatabaseInternal(true));
        }

        private async Task CanRecoverEncryptedDatabaseInternal(bool nullifyMasterKey = false, bool compressDocuments = false)
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
                ModifyDatabaseRecord = record =>
                {
                    record.Encrypted = true;
                    if (compressDocuments)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration
                        {
                            Collections = new[] { "Orders", "Employees", "Companies", "Products" },
                            CompressRevisions = true
                        };
                    }
                },
                Path = dbPath
            }))
            {
                await CreateLegacyNorthwindDatabase(store);

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

                // Temporary dump: RavenDB-13765
                var msg = new StringBuilder();
                if (databaseStatistics.CountOfAttachments + 1 != currentStats.CountOfAttachments ||
                    databaseStatistics.CountOfDocuments + 1 != currentStats.CountOfDocuments)
                {
                    using (var session = store.OpenSession())
                    {
                        var list1 = session.Query<Employee>().ToList();
                        var list2 = session.Query<Category>().ToList();

                        foreach (Employee l1 in list1)
                        {
                            using (var opGetAttach = session.Advanced.Attachments.Get(l1, "photo.jpg"))
                            {
                                var revCnt = session.Advanced.Revisions.GetFor<Employee>(l1.Id).Count;
                                msg.AppendLine($"Employee {l1.Id} Attachment = {opGetAttach?.Details?.DocumentId}, RevCount=" + revCnt);
                            }
                        }
                        foreach (Category l2 in list2)
                        {
                            using (var opGetAttach = session.Advanced.Attachments.Get(l2, "image.jpg"))
                            {
                                var revCnt = session.Advanced.Revisions.GetFor<Employee>(l2.Id).Count;
                                msg.AppendLine($"Category {l2.Id} Attachment = {opGetAttach?.Details?.DocumentId}, RevCount=" + revCnt);
                            }
                        }

                        var documentDatabase = await GetDatabase(store.Database);
                        using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsStorage.AttachmentsSchema, AttachmentsStorage.AttachmentsMetadataSlice);
                            var count = table.NumberOfEntries;
                            var tree = context.Transaction.InnerTransaction.CreateTree(AttachmentsStorage.AttachmentsSlice);
                            var streamsCount = tree.State.NumberOfEntries;
                            msg.AppendLine($"count={count}, streamsCount={streamsCount}");

                            using (var it = tree.Iterate(false))
                            {
                                if (it.Seek(Slices.BeforeAllKeys))
                                {
                                    do
                                    {
                                        msg.AppendLine("tree key=" + it.CurrentKey.Content);
                                    } while (it.MoveNext());
                                }
                            }
                        }
                    }
                    var currentStats2 = store.Maintenance.Send(new GetStatisticsOperation());
                    msg.AppendLine("Get again currentStats.CountOfAttachments=" + currentStats2.CountOfAttachments);
                    throw new Exception(msg.ToString());
                }

                // + 1 as recovery adds some artificial items
                Assert.Equal(databaseStatistics.CountOfAttachments + 1, currentStats.CountOfAttachments);
                Assert.Equal(databaseStatistics.CountOfDocuments + 1, currentStats.CountOfDocuments);
            }
        }
    }
}
