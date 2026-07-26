using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27049 : RavenTestBase
    {
        public RavenDB_27049(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CreateDatabaseOverExistingData_RestoresPersistedSupportedFeatures()
        {
            var path = NewDataPath();

            using (var store = GetDocumentStore(new Options { RunInMemory = false, Path = path }))
            {
                var original = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).SupportedFeatures;
                Assert.Contains(Constants.DatabaseRecord.SupportedFeatures.ThrowRevisionKeyTooBigFix, original);

                // Load the database so the birth feature list is persisted into the documents env.
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.Equal(original.OrderBy(x => x), ReadPersistedSupportedFeatures(database).OrderBy(x => x));

                await SoftDeleteAndRecreateAsync(store, path);

                var adopted = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).SupportedFeatures;
                Assert.Equal(original.OrderBy(x => x), adopted.OrderBy(x => x));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CreateDatabaseOverExistingDataWithControlCharacterIds_UpdatesKeepWorking()
        {
            const string idWithControlCharacter = "users/1\u0001";
            var path = NewDataPath();

            using (var store = GetDocumentStore(AllowControlCharactersInIdentifier(new Options { RunInMemory = false, Path = path })))
            {
                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Legacy" }, idWithControlCharacter);
                    await session.SaveChangesAsync();
                }

                await SoftDeleteAndRecreateAsync(store, path);

                // Without adoption the stamped defaults would include ThrowControlCharactersInIdentifier
                // and any further write to this id would throw.
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.DoesNotContain(Constants.DatabaseRecord.SupportedFeatures.ThrowControlCharactersInIdentifier, record.SupportedFeatures);
                Assert.Contains(Constants.DatabaseRecord.SupportedFeatures.ThrowRevisionKeyTooBigFix, record.SupportedFeatures);

                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.False(database.SupportedFeatures.SupportedFeatureTypes.ThrowControlCharactersInIdentifier);

                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(idWithControlCharacter);
                    Assert.NotNull(user);

                    user.Name = "Updated";
                    await session.SaveChangesAsync();
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CreateDatabaseOverPreFixData_AdoptsEmptyFeatures()
        {
            var path = NewDataPath();

            using (var store = GetDocumentStore(new Options { RunInMemory = false, Path = path }))
            {
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);

                // Simulate data written by builds that predate the persisted feature list.
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    context.Transaction.InnerTransaction.ReadTree(DocumentsStorage.GlobalTreeSlice).Delete(DocumentsStorage.SupportedFeaturesKey);
                    tx.Commit();
                }

                await SoftDeleteAndRecreateAsync(store, path);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.True(record.SupportedFeatures == null || record.SupportedFeatures.Count == 0,
                    $"Expected no features on the adopted record but got: [{string.Join(", ", record.SupportedFeatures ?? new List<string>())}]");

                database = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.False(database.SupportedFeatures.SupportedFeatureTypes.ThrowControlCharactersInIdentifier);

                // The adopted database declares no features, so nothing gets persisted - absent stays the marker.
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tree = context.Transaction.InnerTransaction.ReadTree(DocumentsStorage.GlobalTreeSlice);
                    Assert.False(tree.TryRead(DocumentsStorage.SupportedFeaturesKey, out _));
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task PersistedSupportedFeatures_AreWriteOnce()
        {
            var path = NewDataPath();

            using (var store = GetDocumentStore(new Options { RunInMemory = false, Path = path }))
            {
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.Contains(Constants.DatabaseRecord.SupportedFeatures.ThrowControlCharactersInIdentifier, ReadPersistedSupportedFeatures(database));

                // Make the persisted list diverge from the record.
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    context.Transaction.InnerTransaction.ReadTree(DocumentsStorage.GlobalTreeSlice)
                        .Add(DocumentsStorage.SupportedFeaturesKey, Constants.DatabaseRecord.SupportedFeatures.ThrowRevisionKeyTooBigFix);
                    tx.Commit();
                }

                // Reload: PersistSupportedFeatures must not overwrite an already-present list with the record's.
                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
                database = await Databases.GetDocumentDatabaseInstanceFor(store);

                Assert.Equal(new[] { Constants.DatabaseRecord.SupportedFeatures.ThrowRevisionKeyTooBigFix }, ReadPersistedSupportedFeatures(database));
            }
        }

        private async Task SoftDeleteAndRecreateAsync(DocumentStore store, string path)
        {
            await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: false));

            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(store.Database)
            {
                Settings =
                {
                    { RavenConfiguration.GetKey(x => x.Core.RunInMemory), "false" },
                    { RavenConfiguration.GetKey(x => x.Core.DataDirectory), path }
                }
            }));
        }

        private static List<string> ReadPersistedSupportedFeatures(DocumentDatabase database)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
                return DocumentsStorage.ReadSupportedFeatures(context.Transaction.InnerTransaction);
        }
    }
}
