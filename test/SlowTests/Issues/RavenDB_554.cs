using FastTests;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Json.Linq;

namespace SlowTests.Issues
{
    using System.Linq;
    using Raven.Abstractions.Data;
    using Xunit;

    public class RavenDB_554 : RavenTestBase
    {
        public class Person
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }

            public string MiddleName { get; set; }
        }

        [Fact]
        public void IndexEntryFieldShouldNotContainNullValues()
        {
            const string IndexName = "Index1";

            using (var docStore = GetDocumentStore())
            {

                docStore.DatabaseCommands.PutIndex(IndexName, new IndexDefinition
                {
                    Maps = { "from doc in docs select new { doc.FirstName, doc.LastName, Query = new[] { doc.FirstName, doc.LastName, doc.MiddleName } }" }
                });

                
                using (var session = docStore.OpenSession())
                {
                    session.Store(new Person { FirstName = "John", MiddleName = null, LastName = null });
                    session.Store(new Person { FirstName = "William", MiddleName = "Edgard", LastName = "Smith" });
                    session.Store(new Person { FirstName = "Paul", MiddleName = null, LastName = "Smith" });
                    session.SaveChanges();
                }

                using (var session = docStore.OpenSession())
                {
                    session.Query<Person>(IndexName)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    var queryResult = docStore.DatabaseCommands.Query(IndexName, new IndexQuery(docStore.Conventions), false, true);
                    foreach (var result in queryResult.Results)
                    {
                        RavenJToken q;
                        result.TryGetValue("Query", out q);
                        if (q != null)
                        {
                            Assert.False(q.ToString().Contains(Constants.NullValue));
                        }
                    }
                }
            }
        }
    }
}
