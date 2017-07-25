using System;
using FastTests;
using Xunit;
using System.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;

namespace SlowTests.Bugs.Indexing
{
    public class ComplexLinq : RavenTestBase
    {
        [Fact]
        public void QueryOnNegation()
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
                                                                  x.FirstName != first &&
                                                                  x.LastName == last &&
                                                                  x.BirthDate == birthDate &&
                                                                  x.Country == country);
                    
                    var query = GetIndexQuery(queryable);
                    
                    Assert.Equal("FROM Clients WHERE (((exists(FirstName) AND NOT FirstName = :p0 AND LastName = :p1)) AND BirthDate = :p2) AND Country = :p3", query.Query);
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

                    var query = GetIndexQuery(queryable);

                    Assert.Equal("FROM Clients WHERE ((FirstName = :p0 AND LastName = :p1) AND BirthDate = :p2) AND Country = :p3", query.Query);
                    Assert.Equal("Ayende", query.QueryParameters["p0"]);
                    Assert.Equal("Rahien", query.QueryParameters["p1"]);
                    Assert.Equal(birthDate, query.QueryParameters["p2"]);
                    Assert.Equal("Israel", query.QueryParameters["p3"]);

                    queryable.Any();

                }
            }
        }

        private static IndexQuery GetIndexQuery<T>(IQueryable<T> queryable)
        {
            var inspector = (IRavenQueryInspector)queryable;
            return inspector.GetIndexQuery(isAsync: false);
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
