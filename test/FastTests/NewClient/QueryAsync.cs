using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.NewClient
{
    public class QueryAsync : RavenTestBase
    {
        [Fact]
        public async void QueryAsync_Simple()
        {
            // TODO Iftah, when SaveChangesAsync works make sure to use it here and in LoadAsync tests
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new User { Name = "John" }, "users/1");
                    session.Store(new User { Name = "Jane" }, "users/2");
                    session.Store(new User { Name = "Tarzan" }, "users/3");
                    session.SaveChanges();
                }

                using (var newAsyncSession = store.OpenNewAsyncSession())
                {
                    var queryResult = await newAsyncSession.Query<User>()
                        .NewToListAsync();

                    Assert.Equal(queryResult.Count, 3);
                }
            }
        }

        [Fact]
        public async void QueryAsync_With_Where_Clause()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new User { Name = "John" }, "users/1");
                    session.Store(new User { Name = "Jane" }, "users/2");
                    session.Store(new User { Name = "Tarzan" }, "users/3");
                    session.SaveChanges();
                }

                using (var newAsyncSession = store.OpenNewAsyncSession())
                {
                    var queryResult = await newAsyncSession.Query<User>()
                        .Where(x => x.Name.StartsWith("J"))
                        .NewToListAsync();

                    var queryResult2 = await newAsyncSession.Query<User>()
                        .Where(x => x.Name.Equals("Tarzan"))
                        .NewToListAsync();

                    Assert.Equal(queryResult.Count, 2);
                    Assert.Equal(queryResult2.Count, 1);
                }
            }
        }

        [Fact]
        public async void QueryAsync_By_Index()
        {
            using (var store = GetDocumentStore())
            {
                new DogsIndex().Execute(store);

                using (var session = store.OpenNewSession())
                {
                    session.Store(new Dog { Name = "Snoopy", Breed = "Beagle", Color = "White", Age = 6, IsVaccinated = true}, "dogs/1");
                    session.Store(new Dog { Name = "Brian", Breed = "Labrador", Color = "White", Age = 12, IsVaccinated = false }, "dogs/2");
                    session.Store(new Dog { Name = "Django", Breed = "Jack Russel", Color = "Black", Age = 3, IsVaccinated = true }, "dogs/3");
                    session.Store(new Dog { Name = "Beethoven", Breed = "St. Bernard", Color = "Brown", Age = 1, IsVaccinated = false }, "dogs/4");
                    session.Store(new Dog { Name = "Scooby Doo", Breed = "Great Dane", Color = "Brown", Age = 0, IsVaccinated = false }, "dogs/5");
                    session.Store(new Dog { Name = "Old Yeller", Breed = "Black Mouth Cur", Color = "White", Age = 2, IsVaccinated = true }, "dogs/6");
                    session.Store(new Dog { Name = "Benji", Breed = "Mixed", Color = "White", Age = 0, IsVaccinated = false }, "dogs/7");
                    session.Store(new Dog { Name = "Lassie", Breed = "Collie", Color = "Brown", Age = 6, IsVaccinated = true }, "dogs/8");

                    session.SaveChanges();
                }
                
                using (var newAsyncSession = store.OpenNewAsyncSession())
                {
                    WaitForIndexing(store);

                    var queryResult = await newAsyncSession.Query<DogsIndex.Result, DogsIndex>()
                        .Where(x => x.Age > 2 && x.IsVaccinated == false)
                        .NewToListAsync();

                    Assert.Equal(queryResult.Count, 1);
                    Assert.Equal(queryResult[0].Name, "Brian");

                    var queryResult2 = await newAsyncSession.Query<DogsIndex.Result, DogsIndex>()
                        .Where(x => x.Age <= 2 && x.IsVaccinated == false)
                        .NewToListAsync();

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

