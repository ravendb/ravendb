using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Bugs.Stacey
{
    public class Aspects : RavenTestBase
    {
        private readonly Options _options = new Options
        {
            ModifyDocumentStore = documentStore =>
                documentStore.Conventions.CustomizeJsonSerializer = serializer => serializer.TypeNameHandling = TypeNameHandling.All
        };

        [Fact]
        public void Aspects_Can_Be_Installed()
        {
            using (var store = GetDocumentStore(_options))
            {
                // currency
                var currency = new[]
                {
                new Currency {Name = "Money"},
                new Currency {Name = "Points"},
                };

                // assert
                using (var session = store.OpenSession())
                {
                    // ensure optimistic concurrency
                    session.Advanced.UseOptimisticConcurrency = true;

                    // try to insert the apertures into the database
                    session.Store(currency[0], "currencies/1");
                    session.Store(currency[1], "currencies/2");

                    session.SaveChanges();
                }

                // arrange
                var aspects = new[]
                {
                new Aspect {Name = "Strength", Kind = Kind.Attribute, Category = Category.Physical},
                new Aspect {Name = "Stamina", Kind = Kind.Attribute, Category = Category.Physical},
            };

                // assert
                using (var session = store.OpenSession())
                {
                    // ensure optimistic concurrency
                    session.Advanced.UseOptimisticConcurrency = true;

                    // try to insert the aspects into the database
                    session.Store(aspects[0], "aspects/1");
                    session.Store(aspects[1], "aspects/2");

                    session.SaveChanges();

                    // try to query each of the newly inserted aspects.
                    var query = session.Query<Aspect>().ToList();

                    // unit test each aspect.
                    foreach (var aspect in query)
                    {
                        Assert.NotNull(aspect.Id);
                        Assert.NotNull(aspect.Name);
                        Assert.NotNull(aspect.Kind);
                        Assert.NotNull(aspect.Category);
                    }
                }

                // update
                using (var session = store.OpenSession())
                {
                    // ensure optimistic concurrency
                    session.Advanced.UseOptimisticConcurrency = true;

                    // create an array to hold the results.
                    var results = new Aspect[2];

                    // try to query each of the newly inserted aspects.
                    results[0] = session.Load<Aspect>("aspects/1");
                    results[1] = session.Load<Aspect>("aspects/2");

                    // load the flexskill points currency.
                    var points = session.Load<Currency>("currencies/2");

                    results[0].Path = new Path
                    {
                        Steps = new List<Step>
                    {
                        new Step
                        {
                            Cost = 5,
                            Number = 1,
                            Currency = points.Id,
                            Requirements = new List<Requirement>
                            {
                                new Requirement
                                {
                                    What = results[1].Id,
                                    Value = 2
                                },
                                new Requirement
                                {
                                    What = points.Id,
                                    Value = 5
                                }
                            }
                        }
                    }
                    };

                    session.SaveChanges();
                }


                // assert
                using (var session = store.OpenSession())
                {
                    // ensure optimistic concurrency
                    session.Advanced.UseOptimisticConcurrency = true;

                    // create an array to hold the results.
                    // try to query each of the newly inserted aspects.
                    var results = session.Include("Path.Steps,Requirements,What").Load<Aspect>("aspects/1");
                    //var results = session.Include<Aspect>(aspect => aspect.Path.Steps[0].Requirements[0].What).Load<Aspect>("aspects/1");


                    // the first requirement should be an aspect
                    var requirements = new Entity[2];

                    requirements[0] = session.Load<Aspect>(results.Path.Steps[0].Requirements[0].What);
                    requirements[1] = session.Load<Currency>(results.Path.Steps[0].Requirements[1].What);

                    Assert.IsType<Aspect>(requirements[0]);
                    Assert.IsType<Currency>(requirements[1]);

                    //Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.SaveChanges();
                }
            }

        }

        [Fact]
        public void Index_can_be_queried()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinitionBuilder<Aspect>("AspectsByName")
                {
                    Map = orders => from order in orders
                                    select new { order.Name }
                }.ToIndexDefinition(store.Conventions)));

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinitionBuilder<Entity>("test")
                {
                    Map = docs => from i in docs.WhereEntityIs<Entity>("Aspects", "Currencies")
                                  select new { i.Name }
                }.ToIndexDefinition(store.Conventions)));

                // arrange
                var aspects = new[]
                {
                new Aspect {Name = "Strength", Kind = Kind.Attribute, Category = Category.Physical},
                new Aspect {Name = "Stamina", Kind = Kind.Attribute, Category = Category.Physical},
                };

                // assert
                using (var session = store.OpenSession())
                {
                    // ensure optimistic concurrency
                    session.Advanced.UseOptimisticConcurrency = true;

                    // try to insert the aspects into the database
                    session.Store(aspects[0]);
                    session.Store(aspects[1]);

                    session.SaveChanges();

                    // try to query each of the newly inserted aspects.
                    var query = session.Query<Aspect>().ToList();

                    // unit test each aspect.
                    foreach (var aspect in query)
                    {
                        Assert.NotNull(aspect.Id);
                        Assert.NotNull(aspect.Name);
                        Assert.NotNull(aspect.Kind);
                        Assert.NotNull(aspect.Category);
                    }
                }

                // assert

                using (var session = store.OpenSession())
                {
                    // ensure optimistic concurrency
                    session.Advanced.UseOptimisticConcurrency = true;

                    // create an array to hold the results.
                    // try to query each of the newly inserted aspects.

                    var results = session.Query<Aspect>("AspectsByName")
                        .Customize(n => n.WaitForNonStaleResults())
                        .Where(n => n.Name == "Strength")
                        .ToList();

                    Assert.NotEmpty(results);
                }

            }
 
        }

        private abstract class Entity
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Aspect : Entity
        {
            public Kind Kind { get; set; }
            public Category Category { get; set; }
            public Path Path { get; set; }
        }

        /// <summary>
        /// Denotes the aspect's polymorphed form.
        /// </summary>
        private enum Kind
        {
            None,
            Statistic,
            Attribute,
            Skill
        }

        private enum Category
        {
            None = 0,
            Physical = 1,
            Mental = 2,
            Spirit = 3
        }

        private class Step
        {
            public int Cost { get; set; }
            public int Number { get; set; }
            public string Currency { get; set; }
            public List<Requirement> Requirements { get; set; }
        }

        private class Currency : Entity
        {
        }

        private class Path
        {
            public List<Step> Steps { get; set; }
        }

        private class Requirement
        {
            public string What { get; set; }
            public int Value { get; set; }
        }
    }
}
