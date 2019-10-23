using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9481 : RavenTestBase
    {
        public RavenDB_9481(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AggregateQueryTest()
        {
            using (var store = GetDocumentStore())
            {
                await new DocsIndex().ExecuteAsync(store);

                if (await ShouldInitData(store))
                {
                    await InitializeData(store);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges(TimeSpan.FromSeconds(30));
                    var aggregationResultsByInt = await session.Query<Doc, DocsIndex>()
                        .AggregateBy(x => x.ByRanges(
                            d => d.IntVal < 2,
                            d => d.IntVal >= 2 && d.IntVal < 5,
                            d => d.IntVal >= 5))
                        .ExecuteAsync();
                    Assert.Equal(3, aggregationResultsByInt["IntVal"].Values.Count);

                    var aggregationResultsByDateTime = await session.Query<Doc, DocsIndex>()
                        .AggregateBy(x => x.ByRanges(
                            d => d.DateVal < new DateTime(2017, 1, 2),
                            d => d.DateVal >= new DateTime(2017, 1, 2) && d.DateVal < new DateTime(2017, 1, 5),
                            d => d.DateVal >= new DateTime(2017, 1, 5)))
                        .ExecuteAsync();
                    Assert.Equal(3, aggregationResultsByDateTime["DateVal"].Values.Count);

                    var aggregationResultsByDateTimeNullable = await session.Query<Doc, DocsIndex>()
                        .AggregateBy(x => x.ByRanges(
                            d => d.DateValNullable < new DateTime(2017, 1, 2),
                            d => d.DateValNullable >= new DateTime(2017, 1, 2) && d.DateValNullable < new DateTime(2017, 1, 5),
                            d => d.DateValNullable >= new DateTime(2017, 1, 5)))
                        .ExecuteAsync();

                    Assert.Equal(3, aggregationResultsByDateTimeNullable["DateValNullable"].Values.Count);
                }
            }
        }

        private async Task<bool> ShouldInitData(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<Doc>("doc/1");
                return doc == null;
            }
        }

        private async Task InitializeData(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 1; i <= 10; i++)
                {
                    await session.StoreAsync(new Doc { Id = "doc/" + i, IntVal = i, DateVal = new DateTime(2017, 1, i), DateValNullable = new DateTime(2017, 1, i) });
                }
                await session.SaveChangesAsync();
            }
        }

        private class Doc
        {
            public string Id { get; set; }
            public int? IntVal { get; set; }
            public DateTime DateVal { get; set; }
            public DateTime? DateValNullable { get; set; }
        }

        private class DocsIndex : AbstractIndexCreationTask<Doc>
        {
            public DocsIndex()
            {
                Map = orders =>
                    from order in orders
                    select new
                    {
                        order.Id,
                        order.IntVal,
                        order.DateVal,
                        order.DateValNullable,
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
