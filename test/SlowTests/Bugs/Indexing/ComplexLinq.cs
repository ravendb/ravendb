using System;
using FastTests;
using Xunit;
using System.Linq;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Indexing
{
    public class ComplexLinq : RavenTestBase
    {
        public ComplexLinq(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void QueryOnNegation(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    string first = "Ayende";
                    string last = "Rahien";
                    DateTime birthDate = new DateTime(2001, 1, 1);
                    string country = "Israel";
                    var queryable = session.Query<Client>().Where(x =>
                                                                  x.FirstName != first &&
                                                                  x.LastName == last &&
                                                                  x.BirthDate == birthDate &&
                                                                  x.Country == country);
                    
                    var query = RavenTestHelper.GetIndexQuery(queryable);
                    
                    Assert.Equal("from 'Clients' where (((FirstName != $p0 and LastName = $p1)) and BirthDate = $p2) and Country = $p3", query.Query);
                    Assert.Equal("Ayende", query.QueryParameters["p0"]);
                    Assert.Equal("Rahien", query.QueryParameters["p1"]);
                    Assert.Equal(birthDate, query.QueryParameters["p2"]);
                    Assert.Equal("Israel", query.QueryParameters["p3"]);

                    queryable.Any();
                }
            }
        }

        [Fact]
        public void QueryOnMultipleItems()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    string first = "Ayende";
                    string last = "Rahien";
                    DateTime birthDate = new DateTime(2001, 1, 1);
                    string country = "Israel";
                    var queryable = session.Query<Client>().Where(x =>
                                                                  x.FirstName == first &&
                                                                  x.LastName == last &&
                                                                  x.BirthDate == birthDate &&
                                                                  x.Country == country);

                    var query = RavenTestHelper.GetIndexQuery(queryable);

                    Assert.Equal("from 'Clients' where ((FirstName = $p0 and LastName = $p1) and BirthDate = $p2) and Country = $p3", query.Query);
                    Assert.Equal("Ayende", query.QueryParameters["p0"]);
                    Assert.Equal("Rahien", query.QueryParameters["p1"]);
                    Assert.Equal(birthDate, query.QueryParameters["p2"]);
                    Assert.Equal("Israel", query.QueryParameters["p3"]);

                    queryable.Any();

                }
            }
        }

        private class Client
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public DateTime BirthDate { get; set; }
            public string Country { get; set; }
        }
    }
}
