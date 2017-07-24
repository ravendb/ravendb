using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class FacetAdvancedAPI : NoDisposalNeeded
    {
        private class Test
        {
            public String Id { get; set; }
            public String Manufacturer { get; set; }
            public DateTime Date { get; set; }
            public Decimal Cost { get; set; }
            public int Quantity { get; set; }
            public Double Price { get; set; }
        }

        [Fact]
        public void CanUseNewAPIToDoMultipleQueries()
        {
            var oldFacets = new List<Facet>
            {
                new Facet {Name = "Manufacturer"},
                new Facet
                {
                    Name = "Cost_D_Range",
                    Mode = FacetMode.Ranges,
                    Ranges =
                    {
                        "[NULL TO 200]",
                        "[200 TO 400]",
                        "[400 TO 600]",
                        "[600 TO 800]",
                        "[800 TO NULL]",
                    }
                },
                new Facet
                {
                    Name = "Price_D_Range",
                    Mode = FacetMode.Ranges,
                    Ranges =
                    {
                        "[NULL TO 9.99]",
                        "[9.99 TO 49.99]",
                        "[49.99 TO 99.99]",
                        "[99.99 TO NULL]",
                    }
                }
            };

            var newFacets = new List<Facet>
            {
                new Facet<Test> {Name = x => x.Manufacturer},
                new Facet<Test>
                {
                    Name = x => x.Cost,
                    Ranges =
                        {
                            x => x.Cost < 200m,
                            x => x.Cost > 200m && x.Cost < 400m,
                            x => x.Cost > 400m && x.Cost < 600m,
                            x => x.Cost > 600m && x.Cost < 800m,
                            x => x.Cost > 800m
                        }
                },
                new Facet<Test>
                {
                    Name = x => x.Price,
                    Ranges =
                    {
                        x => x.Price < 9.99,
                        x => x.Price > 9.99 && x.Price < 49.99,
                        x => x.Price > 49.99 && x.Price < 99.99,
                        x => x.Price > 99.99
                    }
                }
            };

            Assert.Equal(true, AreFacetsEqual(oldFacets[0], newFacets[0]));
            Assert.Equal(true, AreFacetsEqual(oldFacets[1], newFacets[1]));
            Assert.Equal(true, AreFacetsEqual(oldFacets[2], newFacets[2]));
        }

        [Fact]
        public void NewAPIThrowsExceptionsForInvalidExpressions()
        {
            //Create an invalid lambda and check it throws an exception!!
            Assert.Throws<InvalidOperationException>(() =>
                TriggerConversion(new Facet<Test>
                {
                    Name = x => x.Cost,
                    //Ranges can be a single item or && only
                    Ranges = { x => x.Cost > 200m || x.Cost < 400m }
                }));

            Assert.Throws<InvalidOperationException>(() =>
                TriggerConversion(new Facet<Test>
                {
                    Name = x => x.Cost,
                    //Ranges can be > or < only
                    Ranges = { x => x.Cost == 200m }
                }));

            Assert.Throws<InvalidOperationException>(() =>
                TriggerConversion(new Facet<Test>
                {
                    //Facets must contain a Name expression
                    //Name = x => x.Cost,
                    Ranges = { x => x.Cost > 200m }
                }));

            Assert.Throws<InvalidOperationException>(() =>
                TriggerConversion(new Facet<Test>
                {
                    Name = x => x.Cost,
                    //Ranges must be on the same field!!!
                    Ranges = { x => x.Price > 9.99 && x.Cost < 49.99m }
                }));
        }

        [Fact]
        public void AdvancedAPIAdvancedEdgeCases()
        {
            var testDateTime = new DateTime(2001, 12, 5);
            var edgeCaseFacet = new Facet<Test>
            {
                Name = x => x.Date,
                Ranges =
                {
                    x => x.Date < DateTime.Now,
                    x => x.Date < new DateTime(2010, 12, 5) && x.Date > testDateTime
                }
            };

            var facet = TriggerConversion(edgeCaseFacet);
            Assert.Equal(2, facet.Ranges.Count);
            Assert.False(string.IsNullOrWhiteSpace(facet.Ranges[0]));
            Assert.Equal(@"[2001-12-05T00:00:00.0000000 TO 2010-12-05T00:00:00.0000000]", facet.Ranges[1]);
        }

        private bool AreFacetsEqual(Facet left, Facet right)
        {
            return left.Name == right.Name &&
                left.Mode == right.Mode &&
                left.Ranges.Count == right.Ranges.Count &&
                left.Ranges.All(x => right.Ranges.Contains(x));
        }

        private Facet TriggerConversion(Facet<Test> facet)
        {
            //The conversion is done with an implicit cast, 
            //so we remain compatible with the original facet API
            return (Facet)facet;
        }
    }
}
