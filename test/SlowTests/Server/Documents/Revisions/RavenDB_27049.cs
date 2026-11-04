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

                // the storage decides, not the record: this storage was born hashed, so lookups stay hash-only
                Assert.True(database.DocumentsStorage.RevisionsStorage.HashedRevisionPk);
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
