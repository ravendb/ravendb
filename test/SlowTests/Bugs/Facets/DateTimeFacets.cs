using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.Facets;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Facets
{
    public class DateTimeFacets : FacetTestBase
    {
        public DateTimeFacets(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ManualCheck()
        {
            var now = DateTime.Today;
            var dates = new List<DateTime>{
                DateTime.SpecifyKind(now.AddDays(-10), DateTimeKind.Unspecified),
                DateTime.SpecifyKind(now.AddDays(-7), DateTimeKind.Unspecified),
                DateTime.SpecifyKind(now.AddDays(0), DateTimeKind.Unspecified),
                DateTime.SpecifyKind(now.AddDays(7), DateTimeKind.Unspecified)                
            };

            var facetsNewWay = new List<RangeFacet>
                {
                    new RangeFacet<Camera>
                    {
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

            var facetOldSchool = new List<RangeFacet>
            {
                new RangeFacet
                {
                    Ranges = new List<string>
                    {
                        $"DateOfListing < '{dates[0]:o}'",
                        $"DateOfListing > '{dates[0]:o}' and DateOfListing < '{dates[1]:o}'",
                        $"DateOfListing > '{dates[1]:o}' and DateOfListing < '{dates[2]:o}'",
                        $"DateOfListing > '{dates[2]:o}' and DateOfListing < '{dates[3]:o}'",
                        $"DateOfListing > '{dates[3]:o}'"
                    }
                }
            };

            for (int i = 0; i < facetOldSchool.Count; i++)
            {
                var o = facetOldSchool[i];
                var n = facetsNewWay[i];

                Assert.Equal(o.DisplayFieldName, n.DisplayFieldName);
                Assert.Equal(o.Options, n.Options);
                Assert.Equal(o.Ranges.Count, n.Ranges.Count);

                for (int j = 0; j < o.Ranges.Count; j++)
                {
                    Assert.Equal(o.Ranges[j], n.Ranges[j]);
                }

                Assert.Equal(o.Aggregations.Count, n.Aggregations.Count);
                Assert.True(Raven.Client.Extensions.DictionaryExtensions.ContentEquals(o.Aggregations, n.Aggregations));
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void PrestonThinksDateRangeQueryShouldProduceCorrectResultsWhenBuiltWithClient(Options options)
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

            using (var store = GetDocumentStore(options))
            {
                CreateCameraCostIndex(store);

                InsertCameraData(store, cameras);

                var facetsNewWay = new List<RangeFacet>
                {
                    new RangeFacet<Camera>
                    {
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
                var facetOldSchool = new List<RangeFacet>
                {
                    new RangeFacet
                    {
                        Ranges = new List<string>
                        {
                            string.Format("DateOfListing < '{0:yyyy-MM-ddTHH:mm:ss.fffffff}'", dates[0]),
                            string.Format("DateOfListing > '{0:yyyy-MM-ddTHH:mm:ss.fffffff}' AND DateOfListing < '{1:yyyy-MM-ddTHH:mm:ss.fffffff}'", dates[0], dates[1]),
                            string.Format("DateOfListing > '{0:yyyy-MM-ddTHH:mm:ss.fffffff}' AND DateOfListing < '{1:yyyy-MM-ddTHH:mm:ss.fffffff}'", dates[1], dates[2]),
                            string.Format("DateOfListing > '{0:yyyy-MM-ddTHH:mm:ss.fffffff}' AND DateOfListing < '{1:yyyy-MM-ddTHH:mm:ss.fffffff}'", dates[2], dates[3]),
                            string.Format("DateOfListing > '{0:yyyy-MM-ddTHH:mm:ss.fffffff}'", dates[3])
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
                            .AggregateBy(facetsNewWay)
                            .Execute();

                        var facetResultsOldSchool = s.Query<Camera, CameraCostIndex>()
                            .Where(exp)
                            .AggregateBy(facetOldSchool)
                            .Execute();

                        var areFacetsEquiv = AreFacetsEquiv(facetResultsNew, facetResultsOldSchool);
                        if (areFacetsEquiv == false)
                        {

                        }
                        Assert.True(areFacetsEquiv);
                    }
                }
            }

        }

        private bool AreFacetsEquiv(Dictionary<string, FacetResult> left, Dictionary<string, FacetResult> right)
        {
            //check if same number of ranges.
            if (left.Count != right.Count
                // || left.Results.Select(r=> r.Key).Intersect(right.Results.Select(r=> r.Key)).Count() != left.Results.Count
                )
            {
                return false;
            }
            //deeper check onthe ranges.
            if (left.Sum(r => r.Value.Values.Sum(var => var.Count)) != right.Sum(r => r.Value.Values.Sum(var => var.Count)))
            {
                return false;
            }

            //all the way down...
            foreach (var lfr in left)
            {
                var leftFacetResult = lfr.Value;
                var rightFacetResult = right.First(r => r.Key.Contains(lfr.Key) || lfr.Key.Contains(r.Key)).Value;
                for (int i = 0; i < leftFacetResult.Values.Count; i++)
                {
                    if (leftFacetResult.Values[i].Count != rightFacetResult.Values[i].Count)
                    {
                        return false;
                    }
                }

            }

            return true;
        }
    }
}
