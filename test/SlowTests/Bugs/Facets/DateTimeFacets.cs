using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Xunit;

namespace SlowTests.Bugs.Facets
{
    public class DateTimeFacets : FacetTestBase
    {
        [Fact]
        public void ManualCheck()
        {
            var now = DateTime.Today;
            var dates = new List<DateTime>{
                now.AddDays(-10),
                now.AddDays(-7),
                now.AddDays(0),
                now.AddDays(7)
            };

            var facetsNewWay = new List<Facet>
                {
                    new Facet<Camera>
                    {
                        Name = x => x.DateOfListing,
                        Ranges =
                           {
                               x => x.DateOfListing < now.AddDays(-10),
                               x => x.DateOfListing > now.AddDays(-10) && x.DateOfListing < now.AddDays(-7),
                               x => x.DateOfListing > now.AddDays(-7) && x.DateOfListing < now.AddDays(0),
                               x => x.DateOfListing > now.AddDays(0) && x.DateOfListing < now.AddDays(7),
                               x => x.DateOfListing > now.AddDays(7)
                           }
                    }
                };

            var facetOldSchool = new List<Facet>{
                    new Facet
                                     {
                                         Name = "DateOfListing",
                                         Mode = FacetMode.Ranges,
                                         Ranges = new List<string>{
                                             string.Format("[NULL TO {0:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff}]", dates[0]),
                                             string.Format("[{0:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff} TO {1:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff}]", dates[0], dates[1]),
                                             string.Format("[{0:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff} TO {1:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff}]", dates[1], dates[2]),
                                             string.Format("[{0:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff} TO {1:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff}]", dates[2], dates[3]),
                                             string.Format("[{0:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff} TO NULL]", dates[3])
                                         }
                                     }
                };

            for (int i = 0; i < facetOldSchool.Count; i++)
            {
                var o = facetOldSchool[i];
                var n= facetsNewWay[i];
                Assert.Equal(o.Name, n.Name);
                Assert.Equal(o.AggregationField, n.AggregationField);
                Assert.Equal(o.DisplayName, n.DisplayName);
                Assert.Equal(o.Aggregation, n.Aggregation);
                Assert.Equal(o.IncludeRemainingTerms, n.IncludeRemainingTerms);
                Assert.Equal(o.MaxResults, n.MaxResults);
                Assert.Equal(o.Mode, n.Mode);
                Assert.Equal(o.TermSortMode, n.TermSortMode);
                Assert.Equal(o.Ranges.Count, n.Ranges.Count);

                for (int j = 0; j < o.Ranges.Count; j++)
                {
                    Assert.Equal(o.Ranges[i], n.Ranges[i]);
                }
            }
        }

        [Fact]
        public void PrestonThinksDateRangeQueryShouldProduceCorrectResultsWhenBuiltWithClient()
        {
            var cameras = GetCameras(30);
            var now = DateTime.Today;

            //putting some in the past and some in the future
            for (int x = 0; x < cameras.Count; x++)
            {
                cameras[x].DateOfListing = now.AddDays(x - 15);
            }


            var dates = new List<DateTime>{
                now.AddDays(-10),
                now.AddDays(-7),
                now.AddDays(0),
                now.AddDays(7)
            };

            using (var store = GetDocumentStore())
            {
                CreateCameraCostIndex(store);

                InsertCameraData(store, cameras);

                var facetsNewWay = new List<Facet>
                {
                    new Facet<Camera>
                    {
                        Name = x => x.DateOfListing,
                        Ranges =
                           {
                               x => x.DateOfListing < now.AddDays(-10),
                               x => x.DateOfListing > now.AddDays(-10) && x.DateOfListing < now.AddDays(-7),
                               x => x.DateOfListing > now.AddDays(-7) && x.DateOfListing < now.AddDays(0),
                               x => x.DateOfListing > now.AddDays(0) && x.DateOfListing < now.AddDays(7),
                               x => x.DateOfListing > now.AddDays(7)
                           }
                    }
                };
                var facetOldSchool = new List<Facet>{
                    new Facet
                                     {
                                         Name = "DateOfListing",
                                         Mode = FacetMode.Ranges,
                                        Ranges = new List<string>{
                                             string.Format("[NULL TO {0:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff}]", dates[0]),
                                             string.Format("[{0:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff} TO {1:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff}]", dates[0], dates[1]),
                                             string.Format("[{0:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff} TO {1:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff}]", dates[1], dates[2]),
                                             string.Format("[{0:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff} TO {1:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff}]", dates[2], dates[3]),
                                             string.Format("[{0:yyyy\\\\-MM\\\\-ddTHH\\\\:mm\\\\:ss.fffffff} TO NULL]", dates[3])
                                         }
                                     }
                };

                using (var s = store.OpenSession())
                {
                    var expressions = new Expression<Func<Camera, bool>>[]
                    {
                        x => x.Cost >= 100 && x.Cost <= 300,
                        x => x.DateOfListing > new DateTime(2000, 1, 1),
                        x => x.Megapixels > 5.0m && x.Cost < 500,
                        x => x.Manufacturer == "abc&edf"
                    };

                    foreach (var exp in expressions)
                    {
                        var facetResultsNew = s.Query<Camera, CameraCostIndex>()
                            .Where(exp)
                            .ToFacets(facetsNewWay);

                         var facetResultsOldSchool = s.Query<Camera, CameraCostIndex>()
                            .Where(exp)
                            .ToFacets(facetOldSchool);

                        var areFacetsEquiv = AreFacetsEquiv(facetResultsNew, facetResultsOldSchool);
                        if (areFacetsEquiv == false)
                        {
                            
                        }
                        Assert.True(areFacetsEquiv);
                    }
                }
            }

        }

        private bool AreFacetsEquiv(FacetedQueryResult left, FacetedQueryResult right)
        {
            //check if same number of ranges.
            if(left.Results.Count != right.Results.Count 
               // || left.Results.Select(r=> r.Key).Intersect(right.Results.Select(r=> r.Key)).Count() != left.Results.Count
                ){
                return false;
            }
            //deeper check onthe ranges.
            if(left.Results.Sum(r=> r.Value.Values.Sum(var=> var.Hits)) != right.Results.Sum(r=> r.Value.Values.Sum(var=> var.Hits))){
                return false;
            }

            //all the way down...
            foreach (var lfr in left.Results)
            {
                var leftFacetResult = lfr.Value;
                var rightFacetResult = right.Results.First(r => r.Key.Contains(lfr.Key) || lfr.Key.Contains(r.Key)).Value;
                for (int i = 0; i < leftFacetResult.Values.Count; i++)
                {
                    if (leftFacetResult.Values[i].Hits != rightFacetResult.Values[i].Hits)
                    {
                        return false;
                    }
                }

            }

            return true;
        }
    }
}
