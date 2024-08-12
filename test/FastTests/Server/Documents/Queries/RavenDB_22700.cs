using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static FastTests.Server.Documents.Queries.DocumentQueryWithDefaultOperator;

namespace FastTests.Server.Documents.Queries
{
    public class RavenDB_22700 : RavenTestBase
    {
        public RavenDB_22700(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void JsQueryWithFilterOnNull(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "EGR", Gender = 'M', Age = 34 }, "Persons/77-A");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<dynamic>(@"
declare function output(doc) {
    var rows123 = [{}];
var test = null;
    return { 
        Rows : [{}].map(row=>({row:row, myRows:test.filter(x=>x)
        })).map(__rvn4=>({
            Custom:__rvn4.myRows[0].Custom
            }))
            };
}
from People as doc where id() = ""Persons/77-A"" select output(doc)
");
                    var results = query.ToList();
                    Assert.Equal(1, results.Count);
                    Assert.Null(results[0].Rows[0].Custom.Value);
                }
            }
        }
    }
}
