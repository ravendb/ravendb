using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12345 : RavenTestBase
    {
        public RavenDB_12345(ITestOutputHelper output) : base(output)
        {
        }

        private class Dog
        {
            public string Name { get; set; }
            public string Breed { get; set; }
            public string Color { get; set; }
            public int Age { get; set; }
            public bool IsVaccinated { get; set; }
        }

        private class DogsIndex : AbstractIndexCreationTask<Dog>
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public int Age { get; set; }
                public bool IsVaccinated { get; set; }
            }

            public DogsIndex()
            {
                Map = dogs => from dog in dogs
                              select new
                              {
                                  dog.Name,
                                  dog.Age,
                                  dog.IsVaccinated
                              };
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void OrderByOnIdFieldShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new DogsIndex().Execute(store);
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new Dog { Name = "dogs1", Breed = "Beagle", Color = "White", Age = 6, IsVaccinated = true }, "dogs1");
                    newSession.Store(new Dog { Name = "dogs2", Breed = "Labrador", Color = "White", Age = 12, IsVaccinated = false }, "dogs2");
                    newSession.Store(new Dog { Name = "dogs10", Breed = "Beagle", Color = "White", Age = 6, IsVaccinated = true }, "dogs10");
                    newSession.SaveChanges();
                }
                using (var newSession = store.OpenSession())
                {
                    var queryResult = newSession.Query<DogsIndex.Result, DogsIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name != "dogs1")
                        .OrderBy(y => y.Id, OrderingType.AlphaNumeric)
                        .ToList();

                    Assert.Equal(queryResult[0].Name, "dogs2");
                    Assert.Equal(queryResult[1].Name, "dogs10");

                    queryResult = newSession.Query<DogsIndex.Result, DogsIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name != "dogs1")
                        .OrderByDescending(y => y.Id, OrderingType.AlphaNumeric)
                        .ToList();

                    Assert.Equal(queryResult[0].Name, "dogs10");
                    Assert.Equal(queryResult[1].Name, "dogs2");
                }
            }
        }
    }
}
