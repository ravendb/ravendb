using System.Linq;
using FastTests;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_554 : RavenNewTestBase
    {
        private class Person
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

                docStore.Admin.Send(new PutIndexOperation(IndexName, new IndexDefinition
                {
                    Maps = { "from doc in docs select new { doc.FirstName, doc.LastName, Query = new[] { doc.FirstName, doc.LastName, doc.MiddleName } }" }
                }));

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

                    using (var commands = docStore.Commands())
                    {
                        var queryResult = commands.Query(IndexName, new IndexQuery(docStore.Conventions), false, true);
                        foreach (BlittableJsonReaderObject result in queryResult.Results)
                        {
                            string q;
                            if (result.TryGet("Query", out q))
                            {
                                Assert.False(q.Contains(Constants.NullValue));
                            }
                        }
                    }
                }
            }
        }
    }
}
