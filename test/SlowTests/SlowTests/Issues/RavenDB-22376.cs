using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SlowTests.Issues
{
    internal class RavenDB_22376 : RavenTestBase
    {
        public RavenDB_22376(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Revisions | RavenTestCategory.ClientApi)]
        public async Task ShardingGetRevisionByCV()
        {
            using var store = Sharding.GetDocumentStore();

            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);


            for (int i = 0; i < 2; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Doc { Name = i.ToString() }, "Docs/1");
                    await session.SaveChangesAsync();
                }
            }

            string cv = null;

            using (var session = store.OpenAsyncSession())
            {
                (await session.Advanced.Revisions.GetMetadataForAsync("Docs/1")).First().TryGetValue(Constants.Documents.Metadata.ChangeVector, out cv);
                Assert.NotNull(cv);
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.Advanced.Revisions.GetAsync<Doc>(cv);
                Assert.NotNull(doc);
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.Advanced.Revisions.GetAsync<Doc>(cv);
                Assert.NotNull(doc); // Fail here
            }

        }

        private class Doc
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
