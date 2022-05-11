using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Platform;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18496 : ReplicationTestBase
    {
        public RavenDB_18496(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public async Task ReplicationShouldNotGetStuckWhenEncryptionBufferSizeIsGreaterThanMaxSizeToSend()
        {
            Encryption.EncryptedServer(out var certificates, out var databaseName);

            using (var encryptedStore = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => databaseName,
                ClientCertificate = certificates.ServerCertificate.Value,
                AdminCertificate = certificates.ServerCertificate.Value,
                Encrypted = true
            }))
            using (var store2 = GetDocumentStore(new Options { ClientCertificate = certificates.ServerCertificate.Value }))
            {
                var db = await Databases.GetDocumentDatabaseInstanceFor(encryptedStore);
                var maxSizeToSend = new Size(16, SizeUnit.Kilobytes);
                db.Configuration.Replication.MaxSizeToSend = maxSizeToSend;

                const string docId = "users/1";
                using (var session = encryptedStore.OpenAsyncSession())
                {
                    var entity = new
                    {
                        Data = Sodium.GenerateRandomBuffer(20 * 1024)
                    };

                    await session.StoreAsync(entity, docId);
                    await session.SaveChangesAsync();
                }

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var doc = db.DocumentsStorage.GetDocumentsFrom(ctx, 0).SingleOrDefault();
                    Assert.NotNull(doc);
                    Assert.Equal(docId, doc.Id);

                    long maxSize = maxSizeToSend.GetValue(SizeUnit.Bytes);
                    long encryptionBufferSize = ctx.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes);

                    Assert.True(doc.Data.Size > maxSize);
                    Assert.True(encryptionBufferSize > maxSize);
                }

                await SetupReplicationAsync(encryptedStore, store2);

                Assert.True(WaitForDocument(store2, docId));

                await EnsureNoReplicationLoop(Server, databaseName);
            }
        }
    }
}
