using System.Linq;
using FastTests;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Linq;
using Raven.NewClient.Operations.Databases.Indexes;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_556 : RavenNewTestBase
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
            using (var docStore = GetDocumentStore())
            {
                using (var session = docStore.OpenSession())
                {
                    session.Store(new Person { FirstName = "John", MiddleName = null, LastName = null });
                    session.Store(new Person { FirstName = "William", MiddleName = "Edgard", LastName = "Smith" });
                    session.Store(new Person { FirstName = "Paul", MiddleName = null, LastName = "Smith" });
                    session.SaveChanges();
                }

                using (var session = docStore.OpenSession())
                {
                    var oldIndexes = session
                        .Advanced
                        .DocumentStore
                        .Admin
                        .Send(new GetIndexNamesOperation(0, 100));

                    session.Query<Person>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "John" || x.FirstName == "Paul")
                        .ToList();

                    var newIdnexes = session
                        .Advanced
                        .DocumentStore
                        .Admin
                        .Send(new GetIndexNamesOperation(0, 100));

                    var newIndex = newIdnexes.Except(oldIndexes).Single();

                    using (var commands = session.Advanced.DocumentStore.Commands())
                    {
                        var queryResult = commands
                            .Query(newIndex, new IndexQuery(docStore.Conventions), false, true);

                        foreach (BlittableJsonReaderObject result in queryResult.Results)
                        {
                            string firstName;
                            Assert.True(result.TryGet("FirstName", out firstName));
                            Assert.NotNull(firstName);
                            Assert.True(new[] {"john", "william", "paul"}.Contains(firstName));
                        }
                    }
                }

            }
        }
    }
}
