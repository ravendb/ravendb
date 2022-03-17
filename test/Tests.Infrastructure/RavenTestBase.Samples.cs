using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using FastTests.Graph;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly SamplesTestBase Samples;

    public class SamplesTestBase
    {
        private readonly RavenTestBase _parent;

        public SamplesTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public void CreateNorthwindDatabase(IDocumentStore store, DatabaseItemType operateOnTypes = DatabaseItemType.Documents)
        {
            store.Maintenance.Send(new CreateSampleDataOperation(operateOnTypes));
        }

        public async Task CreateLegacyNorthwindDatabaseAsync(IDocumentStore store)
        {
            await using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("Tests.Infrastructure.Data.Northwind.4.2.ravendbdump"))
            {
                Assert.NotNull(stream);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }
        }

        public void CreateSimpleData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var entityA = new Entity { Id = "entity/1", Name = "A" };
                var entityB = new Entity { Id = "entity/2", Name = "B" };
                var entityC = new Entity { Id = "entity/3", Name = "C" };

                session.Store(entityA);
                session.Store(entityB);
                session.Store(entityC);

                entityA.References = entityB.Id;
                entityB.References = entityC.Id;
                entityC.References = entityA.Id;

                session.SaveChanges();
            }
        }

        public void CreateDogDataWithCycle(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var arava = new Dog { Name = "Arava" }; //dogs/1
                var oscar = new Dog { Name = "Oscar" }; //dogs/2
                var pheobe = new Dog { Name = "Pheobe" }; //dogs/3

                session.Store(arava);
                session.Store(oscar);
                session.Store(pheobe);

                arava.Likes = new[] { oscar.Id };
                oscar.Likes = new[] { pheobe.Id };
                pheobe.Likes = new[] { arava.Id };

                session.SaveChanges();
            }
        }

        public void CreateDogDataWithoutEdges(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var arava = new Dog { Name = "Arava" }; //dogs/1
                var oscar = new Dog { Name = "Oscar" }; //dogs/2
                var pheobe = new Dog { Name = "Pheobe" }; //dogs/3

                session.Store(arava);
                session.Store(oscar);
                session.Store(pheobe);

                session.SaveChanges();
            }
        }

        public void CreateDataWithMultipleEdgesOfTheSameType(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var arava = new Dog { Name = "Arava" }; //dogs/1
                var oscar = new Dog { Name = "Oscar" }; //dogs/2
                var pheobe = new Dog { Name = "Pheobe" }; //dogs/3

                session.Store(arava);
                session.Store(oscar);
                session.Store(pheobe);

                //dogs/1 => dogs/2
                arava.Likes = new[] { oscar.Id };
                arava.Dislikes = new[] { pheobe.Id };

                //dogs/2 => dogs/2,dogs/3 (cycle!)
                oscar.Likes = new[] { oscar.Id, pheobe.Id };
                oscar.Dislikes = new string[0];

                //dogs/3 => dogs/2
                pheobe.Likes = new[] { oscar.Id };
                pheobe.Dislikes = new[] { arava.Id };

                session.SaveChanges();
            }
        }

        public void CreateMoviesData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var scifi = new Genre
                {
                    Id = "genres/1",
                    Name = "Sci-Fi"
                };

                var fantasy = new Genre
                {
                    Id = "genres/2",
                    Name = "Fantasy"
                };

                var adventure = new Genre
                {
                    Id = "genres/3",
                    Name = "Adventure"
                };

                session.Store(scifi);
                session.Store(fantasy);
                session.Store(adventure);

                var starwars = new Movie
                {
                    Id = "movies/1",
                    Name = "Star Wars Ep.1",
                    Genres = new List<string>
                    {
                        "genres/1",
                        "genres/2"
                    }
                };

                var firefly = new Movie
                {
                    Id = "movies/2",
                    Name = "Firefly Serenity",
                    Genres = new List<string>
                    {
                        "genres/2",
                        "genres/3"
                    }
                };

                var indianaJones = new Movie
                {
                    Id = "movies/3",
                    Name = "Indiana Jones and the Temple Of Doom",
                    Genres = new List<string>
                    {
                        "genres/3"
                    }
                };

                session.Store(starwars);
                session.Store(firefly);
                session.Store(indianaJones);

                session.Store(new User
                {
                    Id = "users/1",
                    Name = "Jack",
                    HasRated = new List<User.Rating>
                    {
                        new User.Rating
                        {
                            Movie = "movies/1",
                            Score = 5
                        },
                        new User.Rating
                        {
                            Movie = "movies/2",
                            Score = 7
                        }
                    }
                });

                session.Store(new User
                {
                    Id = "users/2",
                    Name = "Jill",
                    HasRated = new List<User.Rating>
                    {
                        new User.Rating
                        {
                            Movie = "movies/2",
                            Score = 7
                        },
                        new User.Rating
                        {
                            Movie = "movies/3",
                            Score = 9
                        }
                    }
                });

                session.Store(new User
                {
                    Id = "users/3",
                    Name = "Bob",
                    HasRated = new List<User.Rating>
                    {
                        new User.Rating
                        {
                            Movie = "movies/3",
                            Score = 5
                        }
                    }
                });

                session.SaveChanges();
            }
        }
    }
}
