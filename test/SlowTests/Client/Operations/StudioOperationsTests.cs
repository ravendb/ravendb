using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
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
