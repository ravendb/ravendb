using Tests.Infrastructure;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13252 : RavenTestBase
    {
        public RavenDB_13252(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanUseSpacesInCounterAndCompareExchangeNames(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };

                    session.Store(company, "companies/1");

                    session.CountersFor(company).Increment("Total Likes", 11);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("Total Uses", 55);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(11, session.CountersFor("companies/1").Get("Total Likes"));
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    Assert.Equal(55, session.Advanced.ClusterTransaction.GetCompareExchangeValue<int>("Total Uses").Value);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Company>()
                        .Select(x => new
                        {
                            TotalUses = RavenQuery.CmpXchg<int>("Total Uses")
                        });

                    var queryAsString = query.ToString();
                    Assert.Contains("cmpxchg(\"Total Uses\")", queryAsString);

                    var result = query.First();
                    Assert.Equal(55, result.TotalUses);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Company>()
                        .Select(x => new
                        {
                            Name = x.Name + " " + x.Name,
                            TotalLikes = RavenQuery.Counter(x, "Total Likes")
                        });

                    var queryAsString = query.ToString();
                    Assert.Contains("counter(x, \"Total Likes\")", queryAsString);

                    var result = query.First();
                    Assert.Equal(11, result.TotalLikes);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Company>()
                        .Select(x => new
                        {
                            TotalLikes = RavenQuery.Counter(x, "Total Likes")
                        });

                    var queryAsString = query.ToString();
                    Assert.Contains("counter(x, 'Total Likes')", queryAsString);

                    var result = query.First();
                    Assert.Equal(11, result.TotalLikes);
                }
            }
        }
    }
}
