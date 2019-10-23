using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries.Facets;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Facets
{
    public class FacetsCreationTest : FacetTestBase
    {
        public FacetsCreationTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void PrestonThinksFacetsShouldAllowSimpleExpressionsDuringCreation()
        {
            var now = DateTime.Now;

            var dates = new List<DateTime>{
                now.AddDays(-10),
                now.AddDays(-7),
                now.AddDays(0),
                now.AddDays(7)
            };

            var exceptionThrown = false;
            try
            {
                var facet =new List<RangeFacet>{ new RangeFacet<Camera>
                {
                    Ranges =
                        {
                            x => x.DateOfListing <= dates[0],
                            x => x.DateOfListing > dates[0] && x.DateOfListing < dates[1],
                            x => x.DateOfListing > dates[1] && x.DateOfListing < dates[2],
                            x => x.DateOfListing > dates[2] && x.DateOfListing < dates[3],
                            x => x.DateOfListing >= dates[3]
                        }
                }};
                facet = new List<RangeFacet>{new RangeFacet<Camera>{
                    Ranges =
                        {
                            x => x.DateOfListing <= now.AddDays(-10),
                            x => x.DateOfListing > now.AddDays(-10) && x.DateOfListing < now.AddDays(-7),
                            x => x.DateOfListing > now.AddDays(-7) && x.DateOfListing < now.AddDays(0),
                            x => x.DateOfListing > now.AddDays(0) && x.DateOfListing < now.AddDays(7),
                            x => x.DateOfListing >= now.AddDays(7)
                        }
                }};
                facet = new List<RangeFacet>{new RangeFacet<Camera>{
                    Ranges = {
                        x=> x.Cost < Math.Ceiling(100.99m),
                        x=> x.Cost > Math.Ceiling(100.99m) && x.Cost < Math.Ceiling(100.99m) + 50,
                        x=> x.Cost > Math.Ceiling(100.99m) + 50 && x.Cost < Math.Ceiling(100.99m) + 100,
                        x=> x.Cost > Math.Ceiling(100.99m) + 100,
                    }
                }};

            }

            catch{exceptionThrown = true;}
            Assert.False(exceptionThrown);
        }
    }
}
