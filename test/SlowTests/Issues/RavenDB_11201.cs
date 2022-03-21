using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11201 : RavenTestBase
    {
        public RavenDB_11201(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanIndexNullValuesInArrays()
        {
            using (var store = GetDocumentStore())
            {
                new Dogs_Owners().Execute(store);
                new Dogs_Owners_ByBreed().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Dog
                    {
                        Owner = null,
                        Breed = "German Shepherd"
                    });

                    session.Store(new Dog
                    {
                        Owner = "users/1",
                        Breed = "Labrador"
                    });

                    session.Store(new Dog
                    {
                        Owner = "users/3",
                        Breed = "Alabracadabrador"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var dogs = session.Query<Dogs_Owners.Result, Dogs_Owners>()
                        .Where(x => x.Owners == null)
                        .OfType<Dog>()
                        .ToList();

                    Assert.Equal(1, dogs.Count);
                    Assert.Equal("German Shepherd", dogs[0].Breed);

                    var results = session.Query<Dogs_Owners_ByBreed.Result, Dogs_Owners_ByBreed>()
                        .Where(x => x.Owners == null)
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("German Shepherd", results[0].Breed);
                }
            }
        }

        private class Dogs_Owners : AbstractIndexCreationTask
        {
            public class Result
            {
                public string[] Owners { get; set; }
            }

            public override string IndexName => "Dogs/Owners";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from u in docs.Dogs
select new
{
    Owners = new[] { u.Owner }
}"
                    }
                };
            }
        }

        private class Dogs_Owners_ByBreed : AbstractIndexCreationTask
        {
            public class Result
            {
                public string Breed { get; set; }

                public string[] Owners { get; set; }
            }

            public override string IndexName => "Dogs/Owners/ByBreed";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from u in docs.Dogs
select new
{
    u.Breed,
    Owners = new[] { u.Owner }
}"
                    },
                    Reduce = @"from r in results
group r by r.Breed into g
select new
{
    Breed = g.Key,
    Owners = g.SelectMany(x => x.Owners)
}"
                };
            }
        }

        private class Dog
        {
            public string Id { get; set; }

            public string Breed { get; set; }

            public string Owner { get; set; }
        }
    }
}
