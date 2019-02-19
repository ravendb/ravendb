//-----------------------------------------------------------------------
// <copyright file="FacetedIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class FacetedIndex : FacetTestBase
    {
        private readonly IList<Camera> _data;
        private readonly List<Facet> _originalFacets;
        private readonly List<RangeFacet> _originalRangeFacets;
        private readonly List<Facet> _stronglyTypedFacets;
        private readonly List<RangeFacet> _stronglyTypedRangeFacets;

        private const int NumCameras = 1000;

        public FacetedIndex()
        {
            _data = GetCameras(NumCameras);

            _originalFacets = new List<Facet>
            {
                new Facet
                {
                    FieldName = "Manufacturer"
                },
            };

            _originalRangeFacets = new List<RangeFacet>
            {
                //default is term query		                         
                //In Lucene [ is inclusive, { is exclusive
                new RangeFacet()
                {
                    Ranges =
                    {
                        "Cost <= 200",
                        "Cost BETWEEN 200 AND 400",
                        "Cost BETWEEN 400 AND 600",
                        "Cost BETWEEN 600 AND 800",
                        "Cost >= 800"
                    }
                },
                new RangeFacet
                {
                    Ranges =
                    {
                        "Megapixels <= 3",
                        "Megapixels BETWEEN 3 AND 7",
                        "Megapixels BETWEEN 7 AND 10",
                        "Megapixels >= 10",
                    }
                }
            };

            _stronglyTypedFacets = new List<Facet>();
            _stronglyTypedRangeFacets = new List<RangeFacet>();

            foreach (var facetBase in GetFacets())
            {
                if (facetBase is Facet<Camera> f)
                {
                    _stronglyTypedFacets.Add(f);
                }
                else if (facetBase is RangeFacet<Camera> rf)
                {
                    _stronglyTypedRangeFacets.Add(rf);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedSearch_Remotely()
        {
            using (var store = GetDocumentStore())
            {
                ExecuteTest(store, _originalFacets, _originalRangeFacets);
            }
        }

        [Fact]
        public void CanPerformFacetedSearch_Remotely_Asynchronously()
        {
            using (var store = GetDocumentStore())
            {
                ExecuteTestAsynchronously(store, _originalFacets, _originalRangeFacets);
            }
        }

        [Fact]
        public void CanPerformFacetedSearch_Remotely_WithStronglyTypedAPI()
        {
            using (var store = GetDocumentStore())
            {
                ExecuteTest(store, _stronglyTypedFacets, _stronglyTypedRangeFacets);
            }
        }

        [Fact]
        public void CanPerformFacetedSearch_Embedded()
        {
            using (var store = GetDocumentStore())
            {
                ExecuteTest(store, _stronglyTypedFacets, _stronglyTypedRangeFacets);
            }
        }

        [Fact]
        public void CanPerformFacetedSearch_Embedded_WithStronglyTypedAPI()
        {
            using (var store = GetDocumentStore())
            {
                ExecuteTest(store, _stronglyTypedFacets, _stronglyTypedRangeFacets);
            }
        }

        [Fact]
        public void CanPerformFacetedSearch_Remotely_Lazy()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store, _originalFacets, _originalRangeFacets);

                using (var s = store.OpenSession())
                {
                    var expressions = new Expression<Func<Camera, bool>>[]
                    {
                        x => x.Cost >= 100 && x.Cost <= 300,
                        x => x.DateOfListing > new DateTime(2000, 1, 1),
                        x => x.Megapixels > 5.0m && x.Cost < 500
                    };


                    foreach (var exp in expressions)
                    {
                        var oldRequests = s.Advanced.NumberOfRequests;

                        var facetResults = s.Query<Camera>("CameraCost")
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(exp)
                            .AggregateUsing("facets/CameraFacets")
                            .ExecuteLazy();

                        Assert.Equal(oldRequests, s.Advanced.NumberOfRequests);

                        var filteredData = _data.Where(exp.Compile()).ToList();
                        CheckFacetResultsMatchInMemoryData(facetResults.Value, filteredData);

                        Assert.Equal(oldRequests + 1, s.Advanced.NumberOfRequests);
                    }
                }
            }
        }

        [Fact]
        public void CanPerformFacetedSearch_Remotely_Lazy_can_work_with_others()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store, _originalFacets, _originalRangeFacets);

                using (var s = store.OpenSession())
                {
                    var expressions = new Expression<Func<Camera, bool>>[]
                    {
                        x => x.Cost >= 100 && x.Cost <= 300,
                        x => x.DateOfListing > new DateTime(2000, 1, 1),
                        x => x.Megapixels > 5.0m && x.Cost < 500
                    };

                    foreach (var exp in expressions)
                    {
                        var oldRequests = s.Advanced.NumberOfRequests;
                        var load = s.Advanced.Lazily.Load<Camera>(oldRequests.ToString());
                        var facetResults = s.Query<Camera>("CameraCost")
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(exp)
                            .AggregateUsing("facets/CameraFacets")
                            .ExecuteLazy();

                        Assert.Equal(oldRequests, s.Advanced.NumberOfRequests);

                        var filteredData = _data.Where(exp.Compile()).ToList();
                        CheckFacetResultsMatchInMemoryData(facetResults.Value, filteredData);
                        var forceLoading = load.Value;
                        Assert.Equal(oldRequests + 1, s.Advanced.NumberOfRequests);
                    }
                }
            }
        }

        private void ExecuteTest(IDocumentStore store, List<Facet> facetsToUse, List<RangeFacet> rangeFacets)
        {
            Setup(store, facetsToUse, rangeFacets);

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
                    var facetQueryTimer = Stopwatch.StartNew();
                    var facetResults = s.Query<Camera>("CameraCost")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(exp)
                        .AggregateUsing("facets/CameraFacets")
                        .Execute();
                    facetQueryTimer.Stop();

                    var filteredData = _data.Where(exp.Compile()).ToList();
                    CheckFacetResultsMatchInMemoryData(facetResults, filteredData);
                }
            }
        }

        private void ExecuteTestAsynchronously(IDocumentStore store, List<Facet> facetsToUse, List<RangeFacet> rangeFacets)
        {
            Setup(store, facetsToUse, rangeFacets);

            using (var s = store.OpenAsyncSession())
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
                    var facetQueryTimer = Stopwatch.StartNew();
                    var task = s.Query<Camera>("CameraCost")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(exp)
                        .AggregateUsing("facets/CameraFacets")
                        .ExecuteAsync();

                    task.Wait();
                    facetQueryTimer.Stop();
                    var facetResults = task.Result;
                    var filteredData = _data.Where(exp.Compile()).ToList();
                    CheckFacetResultsMatchInMemoryData(facetResults, filteredData);
                }
            }
        }

        private void Setup(IDocumentStore store, List<Facet> facets,  List<RangeFacet> rangeFacets)
        {
            using (var s = store.OpenSession())
            {
                var facetSetupDoc = new FacetSetup { Id = "facets/CameraFacets", Facets = facets, RangeFacets = rangeFacets };
                s.Store(facetSetupDoc);
                s.SaveChanges();
            }

            CreateCameraCostIndex(store);

            InsertCameraData(store, _data);
        }

        private void CheckFacetResultsMatchInMemoryData(
                    Dictionary<string, FacetResult> facetResults,
                    List<Camera> filteredData)
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
            CheckFacetCount(filteredData.Count(x => x.Cost <= 200.0m),
                            costFacets.FirstOrDefault(x => x.Range == "Cost <= 200"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 200.0m && x.Cost <= 400),
                            costFacets.FirstOrDefault(x => x.Range == "Cost between 200 and 400"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 400.0m && x.Cost <= 600.0m),
                            costFacets.FirstOrDefault(x => x.Range == "Cost between 400 and 600"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 600.0m && x.Cost <= 800.0m),
                            costFacets.FirstOrDefault(x => x.Range == "Cost between 600 and 800"));
            CheckFacetCount(filteredData.Count(x => x.Cost >= 800.0m),
                            costFacets.FirstOrDefault(x => x.Range == "Cost >= 800"));

            //Test the Megapixels_Range facets using the same method
            var megapixelsFacets = facetResults["Megapixels"].Values;
            CheckFacetCount(filteredData.Where(x => x.Megapixels <= 3.0m).Count(),
                            megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels <= 3"));
            CheckFacetCount(filteredData.Where(x => x.Megapixels >= 3.0m && x.Megapixels <= 7.0m).Count(),
                            megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels between 3 and 7"));
            CheckFacetCount(filteredData.Where(x => x.Megapixels >= 7.0m && x.Megapixels <= 10.0m).Count(),
                            megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels between 7 and 10"));
            CheckFacetCount(filteredData.Where(x => x.Megapixels >= 10.0m).Count(),
                            megapixelsFacets.FirstOrDefault(x => x.Range == "Megapixels >= 10"));
        }

        private void CheckFacetCount(int expectedCount, FacetValue facets)
        {
            if (expectedCount > 0)
            {
                Assert.NotNull(facets);
                Assert.Equal(expectedCount, facets.Count);
            }
        }
    }
}
