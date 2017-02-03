using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using Newtonsoft.Json;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
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
                            .ToFacets(facets);

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
                            .ToFacets(facets);

                        var filteredData = cameras.Where(exp.Compile()).ToList();

                        CheckFacetResultsMatchInMemoryData(facetResults, filteredData);
                    }
                }
            }
        }

        [Fact]
        public void RemoteDynamicFacetedSearchHonorsConditionalGet()
        {
            using (var store = GetDocumentStore())
            {
                CreateCameraCostIndex(store);

                InsertCameraData(store, GetCameras(1));

                var facets = GetFacets();

                var jsonFacets = JsonConvert.SerializeObject(facets);

                long? firstEtag;

                const string queryUrl = "/queries/CameraCost?query=Manufacturer%253A{0}&facetStart=0&facetPageSize=&op=facets";

                var requestUrl = string.Format(queryUrl, "canon");

                Assert.Equal(HttpStatusCode.OK, ConditionalGetHelper.PerformPost(store, requestUrl, jsonFacets, null, out firstEtag));

                //second request should give 304 not modified
                Assert.Equal(HttpStatusCode.NotModified, ConditionalGetHelper.PerformPost(store, requestUrl, jsonFacets, firstEtag, out firstEtag));

                //change index etag by inserting new doc
                InsertCameraData(store, GetCameras(1));

                long? secondEtag;

                //changing the index should give 200 OK
                Assert.Equal(HttpStatusCode.OK, ConditionalGetHelper.PerformPost(store, requestUrl, jsonFacets, firstEtag, out secondEtag));

                //next request should give 304 not modified
                Assert.Equal(HttpStatusCode.NotModified, ConditionalGetHelper.PerformPost(store, requestUrl, jsonFacets, secondEtag, out secondEtag));
            }
        }

        private void CheckFacetResultsMatchInMemoryData(FacetedQueryResult facetResults, List<Camera> filteredData)
        {
            //Make sure we get all range values
            Assert.Equal(filteredData.GroupBy(x => x.Manufacturer).Count(),
                        facetResults.Results["Manufacturer"].Values.Count());

            foreach (var facet in facetResults.Results["Manufacturer"].Values)
            {
                var inMemoryCount = filteredData.Count(x => x.Manufacturer.ToLower() == facet.Range);
                Assert.Equal(inMemoryCount, facet.Hits);
            }

            //Go through the expected (in-memory) results and check that there is a corresponding facet result
            //Not the prettiest of code, but it works!!!
            var costFacets = facetResults.Results["Cost_Range"].Values;
            CheckFacetCount(filteredData.Count(x => x.Cost <= 200.0m), costFacets.FirstOrDefault(x => x.Range == "[NULL TO Dx200]"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 200.0m && x.Cost <= 400), costFacets.FirstOrDefault(x => x.Range == "[Dx200 TO Dx400]"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 400.0m && x.Cost <= 600.0m), costFacets.FirstOrDefault(x => x.Range == "[Dx400 TO Dx600]"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 600.0m && x.Cost <= 800.0m), costFacets.FirstOrDefault(x => x.Range == "[Dx600 TO Dx800]"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 800.0m), costFacets.FirstOrDefault(x => x.Range == "[Dx800 TO NULL]"));

            //Test the Megapixels_Range facets using the same method
            var megapixelsFacets = facetResults.Results["Megapixels_Range"].Values;
            CheckFacetCount(filteredData.Count(x => x.Megapixels <= 3.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "[NULL TO Dx3]"));
            CheckFacetCount(filteredData.Count(x => x.Megapixels >= 3.0m && x.Megapixels <= 7.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "[Dx3 TO Dx7]"));
            CheckFacetCount(filteredData.Count(x => x.Megapixels >= 7.0m && x.Megapixels <= 10.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "[Dx7 TO Dx10]"));
            CheckFacetCount(filteredData.Count(x => x.Megapixels >= 10.0m), megapixelsFacets.FirstOrDefault(x => x.Range == "[Dx10 TO NULL]"));
        }

        private void CheckFacetCount(int expectedCount, FacetValue facets)
        {
            if (expectedCount > 0)
            {
                Assert.NotNull(facets);
                Assert.Equal(expectedCount, facets.Hits);
            }
        }
    }
}
