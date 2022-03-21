using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17206 : RavenTestBase
    {
        public RavenDB_17206(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task EntryIsNotPresentInMapReduceIndexWithMoreThan128Terms()
        {
            const string locationIdentifier = "locations/12345-A";

            using var store = GetDocumentStore();
            await store.ExecuteIndexAsync(new SearchIndex());

            using var session = store.OpenAsyncSession();
            session.Advanced.MaxNumberOfRequestsPerSession = 10000;
            await session.StoreAsync(new TestObj
            {
                Id = locationIdentifier,
                Prop = "-1"
            });

            await session.SaveChangesAsync();

            Indexes.WaitForIndexing(store);
            WaitForUserToContinueTheTest(store);

            const int i = 1000;
            var list = new[] { "-1" }.Concat(Enumerable.Range(0, i).Select(i => i.ToString()).ToArray());
            var ravenQueryable = session
                .Query<SearchIndex.Entry, SearchIndex>()
                .Where(x => x.Prop.In(list));
            var location = await ravenQueryable
                .OfType<TestObj>()
                .FirstOrDefaultAsync();

            Assert.NotNull(location);

            list = Enumerable.Range(0, i).Select(i => i.ToString()).ToArray();
            ravenQueryable = session
                .Query<SearchIndex.Entry, SearchIndex>()
                .Where(x => x.Prop.In(list) == false);
            location = await ravenQueryable
                .OfType<TestObj>()
                .FirstOrDefaultAsync();

            Assert.True(location != null, $"Max Length {i}"); // <--------------- Fails here --------------
        }

        private class SearchIndex : AbstractMultiMapIndexCreationTask<SearchIndex.Entry>
        {
            public override string IndexName => "Search";

            public SearchIndex()
            {
                AddMap<TestObj>(locations =>
                    from location in locations

                    select new Entry
                    {
                        Prop = location.Prop,
                    }
                );
            }

            public class Entry
            {
                public string Prop { get; set; }
            }
        }

        private class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }
    }
}
