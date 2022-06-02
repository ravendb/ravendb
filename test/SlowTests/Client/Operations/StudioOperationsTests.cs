using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.Studio;
using Raven.Server.Documents.Sharding.Operations;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Operations
{
    public class StudioOperationsTests : RavenTestBase
    {
        public StudioOperationsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Studio)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DeleteStudioCollection(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.TimeSeries | DatabaseItemType.RevisionDocuments | DatabaseItemType.Documents));

                var op = await store.Operations.SendAsync(new DeleteStudioCollectionOperation(null, "Orders", new List<string>(){ "Orders/1-A" }));
                var operationResult = (BulkOperationResult)await op.WaitForCompletionAsync();

                var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                
                Assert.NotNull(stats);
                Assert.Equal(1, stats.Collections["Orders"]);
            }
        }

        [RavenTheory(RavenTestCategory.Studio)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task GetLastChangeVector(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var shard1 = await Sharding.GetShardNumber(store, "user/A-1");
                var shard2 = await Sharding.GetShardNumber(store, "user/B-2");
                var shard3 = await Sharding.GetShardNumber(store, "user/E-3");

                Assert.NotEqual(shard1, shard2);
                Assert.NotEqual(shard1, shard3);
                Assert.NotEqual(shard2, shard3);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "user/A-1");
                    await session.StoreAsync(new User(), "user/B-2");
                    await session.StoreAsync(new User(), "user/E-3");
                    await session.SaveChangesAsync();
                    
                    var user1 = await session.LoadAsync<User>("user/A-1");
                    var user2 = await session.LoadAsync<User>("user/B-2");
                    var user3 = await session.LoadAsync<User>("user/E-3");

                    var cv1 = session.Advanced.GetMetadataFor(user1)[Constants.Documents.Metadata.ChangeVector].ToString();
                    var cv2 = session.Advanced.GetMetadataFor(user2)[Constants.Documents.Metadata.ChangeVector].ToString();
                    var cv3 = session.Advanced.GetMetadataFor(user3)[Constants.Documents.Metadata.ChangeVector].ToString();

                    var res1 = await store.Maintenance.ForShard(shard1).SendAsync(new GetLastChangeVectorOperation("Users"));
                    var res2 = await store.Maintenance.ForShard(shard2).SendAsync(new GetLastChangeVectorOperation("Users"));
                    var res3 = await store.Maintenance.ForShard(shard3).SendAsync(new GetLastChangeVectorOperation("Users"));

                    Assert.Equal(cv1, res1.LastChangeVector);
                    Assert.Equal(cv2, res2.LastChangeVector);
                    Assert.Equal(cv3, res3.LastChangeVector);
                }
            }
        }

        internal class GetLastChangeVectorOperation : IMaintenanceOperation<LastChangeVectorForCollectionResult>
        {
            private readonly string _collection;

            public GetLastChangeVectorOperation(string collection)
            {
                _collection = collection;
            }
            public RavenCommand<LastChangeVectorForCollectionResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new ShardedLastChangeVectorForCollectionOperation.LastChangeVectorForCollectionCommand(_collection);
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Studio)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanGetStudioFooterStatistics(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "SomeDoc" });
                    session.SaveChanges();
                }
                await store.ExecuteIndexAsync(new AllowedUsers());

                Indexes.WaitForIndexing(store);

                var stats = store.Maintenance.Send(new GetStudioFooterStatisticsOperation());

                Assert.NotNull(stats);
                Assert.Equal(1, stats.CountOfIndexes);
                Assert.Equal(2, stats.CountOfDocuments);
                Assert.Equal(0, stats.CountOfIndexingErrors);
                Assert.Equal(0, stats.CountOfStaleIndexes);
            }
        }

        private class User
        {
            public string Name;
        }

        private class AllowedUsers : AbstractIndexCreationTask<User>
        {
            public override string IndexName => "AllowedUsers";

            public AllowedUsers()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Studio)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task CanGetStudioCollectionFields(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var shard1 = await Sharding.GetShardNumber(store, "user/A-1");
                    var shard2 = await Sharding.GetShardNumber(store, "user/B-2");
                    var shard3 = await Sharding.GetShardNumber(store, "user/E-3");

                    Assert.NotEqual(shard1, shard2);
                    Assert.NotEqual(shard1, shard3);
                    Assert.NotEqual(shard2, shard3);

                    //store docs with different fields in same collection and in different shards
                    var o1 = new MultipleFieldsClass1() { Name1 = "SomeName1", Name2 = "SomeName2" };
                    session.Store(o1, "user/A-1");
                    session.Advanced.GetMetadataFor(o1)[Constants.Documents.Metadata.Collection] = "User";

                    var o2 = new MultipleFieldsClass2() { Name3 = "SomeName3", Name4 = "SomeName4" };
                    session.Store(o2, "user/B-2");
                    session.Advanced.GetMetadataFor(o2)[Constants.Documents.Metadata.Collection] = "User";

                    var o3 = new NoFieldsClass();
                    session.Store(o3, "user/E-3");
                    session.Advanced.GetMetadataFor(o3)[Constants.Documents.Metadata.Collection] = "User";
                    session.SaveChanges();
                }

                var collectionFields = await store.Operations.SendAsync(new GetCollectionFieldsOperation("User", ""));

                Assert.NotNull(collectionFields);
                Assert.Equal(5, collectionFields.Count);
            }
        }

        public class GetCollectionFieldsOperation : IOperation<BlittableJsonReaderObject>
        {
            private readonly string _collection;
            private readonly string _prefix;

            public GetCollectionFieldsOperation(string collection, string prefix)
            {
                _collection = collection;
                _prefix = prefix;
            }
            public RavenCommand<BlittableJsonReaderObject> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
            {
                return new GetCollectionFieldsCommand(_collection, _prefix);
            }
        }

        private class MultipleFieldsClass1
        {
            public string Name1;
            public string Name2;
        }

        private class MultipleFieldsClass2
        {
            public string Name3;
            public string Name4;
        }

        private class NoFieldsClass
        {
        }
    }
}
