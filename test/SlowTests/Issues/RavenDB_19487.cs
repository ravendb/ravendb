using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19487 : RavenTestBase
    {
        public RavenDB_19487(ITestOutputHelper output) : base(output)
        {
        }

        class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }


        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task ToQueryableShouldNotChopDeclareFunctionAsync(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj { Prop = "1234" });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var queryable = from r in session.Query<TestObj>()
                    let a = new { Prop = r.Prop }
                    select new { A = a };

                var queryableStr = queryable.ToString();

                var documentQuery = queryable.ToAsyncDocumentQuery();
                var documentQueryStr = documentQuery.ToString();
                Assert.Equal(queryableStr, documentQueryStr);

                var queryable2 = documentQuery.ToQueryable();
                var queryable2Str = queryable2.ToString();
                Assert.Equal(queryableStr, queryable2Str);
                Assert.Equal(documentQueryStr, queryable2Str);

                var queryableResult = await queryable.ToArrayAsync();
                var documentQueryResult = await documentQuery.ToArrayAsync();
                var queryable2Result = await queryable2.ToArrayAsync();

                Assert.Equivalent(queryableResult, documentQueryResult);
                Assert.Equivalent(queryableResult, queryable2Result);
                Assert.Equivalent(documentQueryResult, queryable2Result);
            }
        }


        [Fact]
        public void ToQueryableShouldNotChopDeclareFunction()
        {
            using var store = GetDocumentStore();
            using (var session = store.OpenSession())
            {
                session.Store(new TestObj { Prop = "1234" });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var queryable = from r in session.Query<TestObj>()
                    let a = new { Prop = r.Prop }
                    select new { A = a };

                var queryableStr = queryable.ToString();

                var documentQuery = queryable.ToDocumentQuery();
                var documentQueryStr = documentQuery.ToString();
                Assert.Equal(queryableStr, documentQueryStr);

                var queryable2 = documentQuery.ToQueryable();
                var queryable2Str = queryable2.ToString();
                Assert.Equal(queryableStr, queryable2Str);
                Assert.Equal(documentQueryStr, queryable2Str);
                var queryableResult = queryable.ToArray();
                var documentQueryResult = documentQuery.ToArray();
                var queryable2Result = queryable2.ToArray();

                Assert.Equivalent(queryableResult, documentQueryResult);
                Assert.Equivalent(queryableResult, queryable2Result);
                Assert.Equivalent(documentQueryResult, queryable2Result);
            }
        }
    }
}
