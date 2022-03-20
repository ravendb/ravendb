using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class QueryAsync : RavenTestBase
    {
        public QueryAsync(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task QueryAsync_Simple()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "John" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Jane" }, "users/2");
                    await asyncSession.StoreAsync(new User { Name = "Tarzan" }, "users/3");
                    await asyncSession.SaveChangesAsync();

                    var queryResult = await asyncSession.Query<User>()
                        .ToListAsync();

                    Assert.Equal(queryResult.Count, 3);
                }
            }
        }

        [Fact]
        public async Task QueryAsync_With_Where_Clause()
        {
            using (var store = GetDocumentStore())
            {
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new User { Name = "John" }, "users/1");
                    await asyncSession.StoreAsync(new User { Name = "Jane" }, "users/2");
                    await asyncSession.StoreAsync(new User { Name = "Tarzan" }, "users/3");
                    await asyncSession.SaveChangesAsync();

                    var queryResult = await asyncSession.Query<User>()
                        .Where(x => x.Name.StartsWith("J"))
                        .ToListAsync();

                    var queryResult2 = await asyncSession.Query<User>()
                        .Where(x => x.Name.Equals("Tarzan"))
                        .ToListAsync();

                    Assert.Equal(queryResult.Count, 2);
                    Assert.Equal(queryResult2.Count, 1);
                }
            }
        }

        [Fact]
        public async Task QueryAsync_By_Index()
        {
            using (var store = GetDocumentStore())
            {
                new DogsIndex().Execute(store);
                
                using (var asyncSession = store.OpenAsyncSession())
                {
                    await asyncSession.StoreAsync(new Dog { Name = "Snoopy", Breed = "Beagle", Color = "White", Age = 6, IsVaccinated = true }, "dogs/1");
                    await asyncSession.StoreAsync(new Dog { Name = "Brian", Breed = "Labrador", Color = "White", Age = 12, IsVaccinated = false }, "dogs/2");
                    await asyncSession.StoreAsync(new Dog { Name = "Django", Breed = "Jack Russel", Color = "Black", Age = 3, IsVaccinated = true }, "dogs/3");
                    await asyncSession.StoreAsync(new Dog { Name = "Beethoven", Breed = "St. Bernard", Color = "Brown", Age = 1, IsVaccinated = false }, "dogs/4");
                    await asyncSession.StoreAsync(new Dog { Name = "Scooby Doo", Breed = "Great Dane", Color = "Brown", Age = 0, IsVaccinated = false }, "dogs/5");
                    await asyncSession.StoreAsync(new Dog { Name = "Old Yeller", Breed = "Black Mouth Cur", Color = "White", Age = 2, IsVaccinated = true }, "dogs/6");
                    await asyncSession.StoreAsync(new Dog { Name = "Benji", Breed = "Mixed", Color = "White", Age = 0, IsVaccinated = false }, "dogs/7");
                    await asyncSession.StoreAsync(new Dog { Name = "Lassie", Breed = "Collie", Color = "Brown", Age = 6, IsVaccinated = true }, "dogs/8");

                    await asyncSession.SaveChangesAsync();

                    Indexes.WaitForIndexing(store);
                }
                using (var asyncSession = store.OpenAsyncSession())
                {
                    var queryResult = await asyncSession.Query<DogsIndex.Result, DogsIndex>()
                        .Where(x => x.Age > 2 && x.IsVaccinated == false)
                        .ToListAsync();

                    Assert.Equal(queryResult.Count, 1);
                    Assert.Equal(queryResult[0].Name, "Brian");

                    var queryResult2 = await asyncSession.Query<DogsIndex.Result, DogsIndex>()
                        .Where(x => x.Age <= 2 && x.IsVaccinated == false)
                        .ToListAsync();

                    Assert.Equal(queryResult2.Count, 3);

                    var list = new List<string>();
                    foreach (var dog in queryResult2)
                    {
                        list.Add(dog.Name);
                    }
                    Assert.True(list.Contains("Beethoven"));
                    Assert.True(list.Contains("Scooby Doo"));
                    Assert.True(list.Contains("Benji"));
                }
            }
        }

        public class Dog
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Breed { get; set; }
            public string Color { get; set; }
            public int Age { get; set; }
            public bool IsVaccinated { get; set; }
        }

        public class DogsIndex : AbstractIndexCreationTask<Dog>
        {
            public class Result
            {
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
    }
}

