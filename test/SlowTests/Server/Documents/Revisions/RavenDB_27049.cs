using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using static Tests.Infrastructure.Utils.RevisionTestHelpers;

namespace SlowTests.Server.Documents.Revisions
{
    public class RavenDB_27049 : RavenTestBase
    {
        public RavenDB_27049(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task CreateDatabaseOverExistingData_RestoresPersistedSupportedFeatures()
        {
            var path = NewDataPath();

            using (var store = GetDocumentStore(new Options { RunInMemory = false, Path = path }))
            {
                var original = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).SupportedFeatures;
                Assert.Contains(Constants.DatabaseRecord.SupportedFeatures.HashedRevisionPk, original);

                // Load the database so the birth feature list is persisted into the documents env.
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.True(database.DocumentsStorage.RevisionsStorage.DualForm.HashOnly);
                Assert.Equal(original.OrderBy(x => x), ReadPersistedSupportedFeatures(database).OrderBy(x => x));

                await SoftDeleteAndRecreateAsync(store, path);

                var adopted = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).SupportedFeatures;
                Assert.Equal(original.OrderBy(x => x), adopted.OrderBy(x => x));

                database = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.True(database.DocumentsStorage.RevisionsStorage.DualForm.HashOnly);
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task CreateDatabaseOverExistingLegacyData_DoesNotStampHashedRevisionPk_LegacyRevisionStaysReachable()
        {
            var path = NewDataPath();

            using (var store = GetDocumentStore(new Options
            {
                RunInMemory = false,
                Path = path,
                ModifyDatabaseRecord = StripHashedRevisionPkToken
            }))
            {
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.False(database.SupportedFeatures.SupportedFeatureTypes.HashedRevisionPk);

                string knownChangeVector = await SeedLegacyRevisionAsync(store, database);

                await SoftDeleteAndRecreateAsync(store, path);

                // Without adoption the fresh record would have been stamped with HashedRevisionPk.
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(new[] { Constants.DatabaseRecord.SupportedFeatures.ThrowRevisionKeyTooBigFix }, record.SupportedFeatures);

                database = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.False(database.DocumentsStorage.RevisionsStorage.DualForm.HashOnly);
                AssertLegacyRevisionReachable(database, knownChangeVector);
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task CreateDatabaseOverPreFixData_AdoptsEmptyFeatures_LegacyRevisionStaysReachable()
        {
            var path = NewDataPath();

            using (var store = GetDocumentStore(new Options { RunInMemory = false, Path = path }))
            {
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);

                string knownChangeVector = await SeedLegacyRevisionAsync(store, database);

                // Simulate data written by builds that predate the persisted feature list.
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    context.Transaction.InnerTransaction.ReadTree(DocumentsStorage.GlobalTreeSlice).Delete(DocumentsStorage.SupportedFeaturesSlice);
                    tx.Commit();
                }

                await SoftDeleteAndRecreateAsync(store, path);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.True(record.SupportedFeatures == null || record.SupportedFeatures.Count == 0,
                    $"Expected no features on the adopted record but got: [{string.Join(", ", record.SupportedFeatures ?? new List<string>())}]");

                database = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.False(database.DocumentsStorage.RevisionsStorage.DualForm.HashOnly);
                AssertLegacyRevisionReachable(database, knownChangeVector);

                // The adopted database declares no features, so nothing gets persisted - absent stays the marker.
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tree = context.Transaction.InnerTransaction.ReadTree(DocumentsStorage.GlobalTreeSlice);
                    Assert.False(tree.TryRead(DocumentsStorage.SupportedFeaturesSlice, out _));
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task PersistedSupportedFeatures_AreWriteOnce()
        {
            var path = NewDataPath();

            using (var store = GetDocumentStore(new Options { RunInMemory = false, Path = path }))
            {
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var birth = ReadPersistedSupportedFeatures(database);
                Assert.Contains(Constants.DatabaseRecord.SupportedFeatures.HashedRevisionPk, birth);

                var requestExecutor = store.GetRequestExecutor(store.Database);
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    await requestExecutor.ExecuteAsync(new ModifySupportedFeaturesCommand(store.Conventions,
                        add: [],
                        remove: [Constants.DatabaseRecord.SupportedFeatures.HashedRevisionPk]), context);
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.DoesNotContain(Constants.DatabaseRecord.SupportedFeatures.HashedRevisionPk, record.SupportedFeatures);

                // Reload: PersistSupportedFeaturesIfAbsent must not overwrite the birth list with the mutated record.
                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
                database = await Databases.GetDocumentDatabaseInstanceFor(store);

                Assert.False(database.SupportedFeatures.SupportedFeatureTypes.HashedRevisionPk);
                Assert.Equal(birth.OrderBy(x => x), ReadPersistedSupportedFeatures(database).OrderBy(x => x));
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
                Assert.Contains(Constants.DatabaseRecord.SupportedFeatures.HashedRevisionPk, record.SupportedFeatures);

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

        private async Task<string> SeedLegacyRevisionAsync(DocumentStore store, DocumentDatabase database)
        {
            using (IAsyncDocumentSession session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Seed" }, "users/1");
                await session.SaveChangesAsync();
            }

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                Raven.Server.Utils.ChangeVector compoundCv = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));

                RevisionLegacyRowSeeder.SeedLegacyRevisionRow(
                    context, database, "users/1", "Users", compoundCv,
                    etag: database.DocumentsStorage.GenerateNextEtag());

                tx.Commit();
                return compoundCv.AsString();
            }
        }

        private static void AssertLegacyRevisionReachable(DocumentDatabase database, string changeVector)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                Document revision = database.DocumentsStorage.RevisionsStorage.GetRevision(context, changeVector);
                Assert.NotNull(revision);
            }
        }

        private async Task SoftDeleteAndRecreateAsync(DocumentStore store, string path)
        {
            await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: false));

            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(RecordOver(store.Database, path)));
        }

        private static DatabaseRecord RecordOver(string databaseName, string path) => new(databaseName)
        {
            Settings =
            {
                { RavenConfiguration.GetKey(x => x.Core.RunInMemory), "false" },
                { RavenConfiguration.GetKey(x => x.Core.DataDirectory), path }
            }
        };

        private static List<string> ReadPersistedSupportedFeatures(DocumentDatabase database)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
                return DocumentsStorage.ReadSupportedFeatures(context.Transaction.InnerTransaction);
        }

        private sealed class ModifySupportedFeaturesCommand(DocumentConventions conventions, string[] add, string[] remove) : RavenCommand
        {
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/features?raft-request-id={RaftIdGenerator.NewId()}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var json = new DynamicJsonValue
                        {
                            ["Add"] = add,
                            ["Remove"] = remove
                        };

                        await ctx.WriteAsync(stream, ctx.ReadObject(json, "database-features")).ConfigureAwait(false);
                    }, conventions)
                };
            }
        }
    }
}
