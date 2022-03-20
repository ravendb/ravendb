using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2314 : RavenTestBase
    {
        public RavenDB_2314(ITestOutputHelper output) : base(output)
        {
        }

        private class Pizzeria
        {
            public string Name { get; set; }

            public string DeliveryArea { get; set; }
        }

        private class SpatialIndex : AbstractIndexCreationTask<Pizzeria>
        {
            public SpatialIndex()
            {
                Map = pizzerias => from pizzeria in pizzerias
                                   select new
                                   {
                                       pizzeria.Name,
                                       pizzeria.DeliveryArea
                                   };

                Spatial(x => x.DeliveryArea, x => x.Geography.QuadPrefixTreeIndex(5));
            }
        }

        [Fact]
        public void Spatial_index_should_not_stop_indexing_after_one_bad_document()
        {
            var validPizzeriaDoc = new Pizzeria
            {
                Name = "Pizza Hot",
                DeliveryArea = "POLYGON ((5 10, 10 5, 15 10, 10 15, 5 10))"
            };

            var anotherValidPizzeriaDoc = new Pizzeria
            {
                Name = "Dominox Pizza",
                DeliveryArea = "POLYGON ((6 10, 10 6, 15 10, 10 15, 6 10))"
            };

            var yetAnotherValidPizzeriaDoc = new Pizzeria
            {
                Name = "Sharky Pizza",
                DeliveryArea = "POLYGON ((7 10, 10 7, 15 10, 10 15, 7 10))"
            };

            var invalidPizzeriaDoc = new Pizzeria
            {
                Name = "Very evil pizza",
                DeliveryArea = "POLYGON ((1 1,  3 3, 1 3,  3 1, 1 1))"
            };

            var invalidPizzeriaDoc2 = new Pizzeria
            {
                Name = "Very evil pizza2",
                DeliveryArea = "POLYGON ((1 1,  3 3, 1 3,  3 1, 1 1))"
            };
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(validPizzeriaDoc);
                    session.Store(anotherValidPizzeriaDoc);
                    session.Store(invalidPizzeriaDoc);
                    session.Store(yetAnotherValidPizzeriaDoc);
                    session.Store(invalidPizzeriaDoc2);
                    session.SaveChanges();
                }

                new SpatialIndex().Execute(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var pizzeriaDocCount = session.Query<Pizzeria, SpatialIndex>().Count();
                    var pizzerias = session.Query<Pizzeria, SpatialIndex>().ToList();
                    var stats = store.Maintenance.Send(new GetStatisticsOperation());
                    /*
                    var indexErrors = store.Admin.Send(new GetIndexErrorsOperation());
                    if (indexErrors.Errors.Length != 2)
                    {
                        Assert.False(true, string.Join(Environment.NewLine, stats.Errors.Select(e => e.ToString())));
                    }

                    Assert.Equal(2, stats.Errors.Length);
                    Assert.Equal("pizzerias/3", stats.Errors.First().Document);
                    Assert.Equal("pizzerias/5", stats.Errors.Last().Document);
                    Assert.NotEmpty(pizzerias);
                    Assert.Equal(3,pizzeriaDocCount);
                    */
                }
            }
        }
    }

}
