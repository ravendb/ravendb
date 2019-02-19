using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class DynamicFacets : FacetTestBase
    {
        [Fact]
        public void CanPerformDynamicFacetedSearch_Embedded()
        {
            var cameras = GetCameras(30);

            using (var store = GetDocumentStore())
            {
                CreateCameraCostIndex(store);

                InsertCameraData(store, cameras);

                var facets = GetFacets();

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
                        var facetResults = s.Query<Camera, CameraCostIndex>()
                            .Where(exp)
                            .AggregateBy(facets)
                            .Execute();

                        var filteredData = cameras.Where(exp.Compile()).ToList();

                        CheckFacetResultsMatchInMemoryData(facetResults, filteredData);
                    }
                }
            }
        }

        [Fact]
        public void CanPerformDynamicFacetedSearch_Remotely()
        {
            using (var store = GetDocumentStore())
            {
                var cameras = GetCameras(30);

                CreateCameraCostIndex(store);

                InsertCameraData(store, cameras);

                var facets = GetFacets();

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
                        var facetResults = s.Query<Camera, CameraCostIndex>()
                            .Where(exp)
                            .AggregateBy(facets)
                            .Execute();

                        var filteredData = cameras.Where(exp.Compile()).ToList();

                        CheckFacetResultsMatchInMemoryData(facetResults, filteredData);
                    }
                }
            }
        }

        private void CheckFacetResultsMatchInMemoryData(Dictionary<string, FacetResult> facetResults, List<Camera> filteredData)
        {
            //Make sure we get all range values
            Assert.Equal(filteredData.GroupBy(x => x.Manufacturer).Count(),
                        facetResults["Manufacturer"].Values.Count());

            foreach (var facet in facetResults["Manufacturer"].Values)
            {
                var inMemoryCount = filteredData.Count(x => x.Manufacturer.ToLower() == facet.Range);
                Assert.Equal(inMemoryCount, facet.Count);
            }

            //Go through the expected (in-memory) results and check that there is a corresponding facet result
            //Not the prettiest of code, but it works!!!
            var costFacets = facetResults["Cost"].Values;
            CheckFacetCount(filteredData.Count(x => x.Cost <= 200.0m), costFacets.FirstOrDefault(x => x.Range == "Cost <= 200.0"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 200.0m && x.Cost <= 400), costFacets.FirstOrDefault(x => x.Range == "Cost between 200.0 and 400.0"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 400.0m && x.Cost <= 600.0m), costFacets.FirstOrDefault(x => x.Range == "Cost between 400.0 and 600.0"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 600.0m && x.Cost <= 800.0m), costFacets.FirstOrDefault(x => x.Range == "Cost between 600.0 and 800.0"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 800.0m), costFacets.FirstOrDefault(x => x.Range == "Cost >= 800.0"));

            //Test the Megapixels_Range facets using the same method
            var megapixelsFacets = facetResults["Megapixels"].Values;
            CheckFacetCount(filteredData.Count(x => x.Megapixels <= 3.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels <= 3.0"));
            CheckFacetCount(filteredData.Count(x => x.Megapixels >= 3.0m && x.Megapixels <= 7.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels between 3.0 and 7.0"));
            CheckFacetCount(filteredData.Count(x => x.Megapixels >= 7.0m && x.Megapixels <= 10.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels between 7.0 and 10.0"));
            CheckFacetCount(filteredData.Count(x => x.Megapixels >= 10.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels >= 10.0"));
        }

        private static void CheckFacetCount(int expectedCount, FacetValue facets)
        {
            if (expectedCount > 0)
            {
                Assert.NotNull(facets);
                Assert.Equal(expectedCount, facets.Count);
            }
        }
    }
}
