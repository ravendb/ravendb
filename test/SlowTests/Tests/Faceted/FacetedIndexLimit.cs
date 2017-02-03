//-----------------------------------------------------------------------
// <copyright file="FacetedIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class FacetedIndexLimit : FacetTestBase
    {
        private readonly IList<Camera> _data;
        private const int NumCameras = 1000;

        public FacetedIndexLimit()
        {
            _data = GetCameras(NumCameras);
        }

        [Fact]
        public void CanPerformSearchWithTwoDefaultFacets()
        {
            var facets = new List<Facet> { new Facet { Name = "Manufacturer" }, new Facet { Name = "Model" } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets");

                    Assert.Equal(5, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal("canon", facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal("jessops", facetResults.Results["Manufacturer"].Values[1].Range);
                    Assert.Equal("nikon", facetResults.Results["Manufacturer"].Values[2].Range);
                    Assert.Equal("phillips", facetResults.Results["Manufacturer"].Values[3].Range);
                    Assert.Equal("sony", facetResults.Results["Manufacturer"].Values[4].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(new List<string>(), facetResults.Results["Manufacturer"].RemainingTerms);
                    Assert.Equal(0, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(0, facetResults.Results["Manufacturer"].RemainingHits);

                    Assert.Equal(5, facetResults.Results["Model"].Values.Count());
                    Assert.Equal("model1", facetResults.Results["Model"].Values[0].Range);
                    Assert.Equal("model2", facetResults.Results["Model"].Values[1].Range);
                    Assert.Equal("model3", facetResults.Results["Model"].Values[2].Range);
                    Assert.Equal("model4", facetResults.Results["Model"].Values[3].Range);
                    Assert.Equal("model5", facetResults.Results["Model"].Values[4].Range);

                    foreach (var facet in facetResults.Results["Model"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Model.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(new List<string>(), facetResults.Results["Model"].RemainingTerms);
                    Assert.Equal(0, facetResults.Results["Model"].RemainingTermsCount);
                    Assert.Equal(0, facetResults.Results["Model"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_TermAsc()
        {
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 2, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .Where(x => x.DateOfListing > new DateTime(2000, 1, 1))
                        .ToFacets("facets/CameraFacets");

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal("canon", facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal("jessops", facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.DateOfListing > new DateTime(2000, 1, 1)).Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(3, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(3, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal("nikon", facetResults.Results["Manufacturer"].RemainingTerms[0]);
                    Assert.Equal("phillips", facetResults.Results["Manufacturer"].RemainingTerms[1]);
                    Assert.Equal("sony", facetResults.Results["Manufacturer"].RemainingTerms[2]);

                    Assert.Equal(_data.Count(x => x.DateOfListing > new DateTime(2000, 1, 1)),
                        facetResults.Results["Manufacturer"].Values[0].Hits +
                        facetResults.Results["Manufacturer"].Values[1].Hits +
                        facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_TermDesc()
        {
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 3, TermSortMode = FacetTermSortMode.ValueDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .Where(x => x.DateOfListing > new DateTime(2000, 1, 1))
                        .ToFacets("facets/CameraFacets");

                    Assert.Equal(3, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal("sony", facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal("phillips", facetResults.Results["Manufacturer"].Values[1].Range);
                    Assert.Equal("nikon", facetResults.Results["Manufacturer"].Values[2].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.DateOfListing > new DateTime(2000, 1, 1)).Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(2, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(2, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal("jessops", facetResults.Results["Manufacturer"].RemainingTerms[0]);
                    Assert.Equal("canon", facetResults.Results["Manufacturer"].RemainingTerms[1]);

                    Assert.Equal(_data.Count(x => x.DateOfListing > new DateTime(2000, 1, 1)),
                        facetResults.Results["Manufacturer"].Values[0].Hits +
                        facetResults.Results["Manufacturer"].Values[1].Hits +
                        facetResults.Results["Manufacturer"].Values[2].Hits +
                        facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_HitsAsc()
        {
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 2, TermSortMode = FacetTermSortMode.HitsAsc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets");

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderBy(x => x.Count)
                        .ThenBy(x => x.Manufacturer).Select(x => x.Manufacturer.ToLower()).ToList();

                    var manufacturer = facetResults.Results["Manufacturer"];
                    Assert.Equal(2, manufacturer.Values.Count());
                    Assert.Equal(camerasByHits[0], manufacturer.Values[0].Range);
                    Assert.Equal(camerasByHits[1], manufacturer.Values[1].Range);

                    foreach (var facet in manufacturer.Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    try
                    {
                        if (manufacturer.RemainingHits == 2)
                        {
                            WaitForUserToContinueTheTest(store, debug: false);
                        }
                        Assert.Equal(3, manufacturer.RemainingTermsCount);
                        Assert.Equal(3, manufacturer.RemainingTerms.Count());
                        Assert.Equal(camerasByHits[2], manufacturer.RemainingTerms[0]);
                        Assert.Equal(camerasByHits[3], manufacturer.RemainingTerms[1]);
                        Assert.Equal(camerasByHits[4], manufacturer.RemainingTerms[2]);

                        Assert.Equal(_data.Count(),
                                     manufacturer.Values[0].Hits +
                                     manufacturer.Values[1].Hits +
                                     manufacturer.RemainingHits);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        WaitForUserToContinueTheTest(store, debug: false);
                        throw;
                    }
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_HitsDesc()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 20, TermSortMode = FacetTermSortMode.HitsDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets");

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).ToList();

                    Assert.Equal(5, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    if (camerasByHits[1] != facetResults.Results["Manufacturer"].Values[1].Range)
                        WaitForUserToContinueTheTest(store, debug: false);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);
                    Assert.Equal(camerasByHits[2], facetResults.Results["Manufacturer"].Values[2].Range);
                    Assert.Equal(camerasByHits[3], facetResults.Results["Manufacturer"].Values[3].Range);
                    Assert.Equal(camerasByHits[4], facetResults.Results["Manufacturer"].Values[4].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(0, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(0, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(0, facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformSearchWithTwoDefaultFacets_LuceneQuery()
        {
            var facets = new List<Facet> { new Facet { Name = "Manufacturer" }, new Facet { Name = "Model" } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets");

                    Assert.Equal(5, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal("canon", facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal("jessops", facetResults.Results["Manufacturer"].Values[1].Range);
                    Assert.Equal("nikon", facetResults.Results["Manufacturer"].Values[2].Range);
                    Assert.Equal("phillips", facetResults.Results["Manufacturer"].Values[3].Range);
                    Assert.Equal("sony", facetResults.Results["Manufacturer"].Values[4].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(new List<string>(), facetResults.Results["Manufacturer"].RemainingTerms);
                    Assert.Equal(0, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(0, facetResults.Results["Manufacturer"].RemainingHits);

                    Assert.Equal(5, facetResults.Results["Model"].Values.Count());
                    Assert.Equal("model1", facetResults.Results["Model"].Values[0].Range);
                    Assert.Equal("model2", facetResults.Results["Model"].Values[1].Range);
                    Assert.Equal("model3", facetResults.Results["Model"].Values[2].Range);
                    Assert.Equal("model4", facetResults.Results["Model"].Values[3].Range);
                    Assert.Equal("model5", facetResults.Results["Model"].Values[4].Range);

                    foreach (var facet in facetResults.Results["Model"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Model.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(new List<string>(), facetResults.Results["Model"].RemainingTerms);
                    Assert.Equal(0, facetResults.Results["Model"].RemainingTermsCount);
                    Assert.Equal(0, facetResults.Results["Model"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_TermAsc_LuceneQuery()
        {
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 2, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .WhereGreaterThan(x => x.DateOfListing, new DateTime(2000, 1, 1))
                        .ToFacets("facets/CameraFacets");

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal("canon", facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal("jessops", facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.DateOfListing > new DateTime(2000, 1, 1)).Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(3, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(3, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal("nikon", facetResults.Results["Manufacturer"].RemainingTerms[0]);
                    Assert.Equal("phillips", facetResults.Results["Manufacturer"].RemainingTerms[1]);
                    Assert.Equal("sony", facetResults.Results["Manufacturer"].RemainingTerms[2]);

                    Assert.Equal(_data.Count(x => x.DateOfListing > new DateTime(2000, 1, 1)),
                        facetResults.Results["Manufacturer"].Values[0].Hits +
                        facetResults.Results["Manufacturer"].Values[1].Hits +
                        facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_TermDesc_LuceneQuery()
        {
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 3, TermSortMode = FacetTermSortMode.ValueDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .WhereGreaterThan(x => x.DateOfListing, new DateTime(2000, 1, 1))
                        .ToFacets("facets/CameraFacets");

                    Assert.Equal(3, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal("sony", facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal("phillips", facetResults.Results["Manufacturer"].Values[1].Range);
                    Assert.Equal("nikon", facetResults.Results["Manufacturer"].Values[2].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.DateOfListing > new DateTime(2000, 1, 1)).Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(2, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(2, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal("jessops", facetResults.Results["Manufacturer"].RemainingTerms[0]);
                    Assert.Equal("canon", facetResults.Results["Manufacturer"].RemainingTerms[1]);

                    Assert.Equal(_data.Count(x => x.DateOfListing > new DateTime(2000, 1, 1)),
                        facetResults.Results["Manufacturer"].Values[0].Hits +
                        facetResults.Results["Manufacturer"].Values[1].Hits +
                        facetResults.Results["Manufacturer"].Values[2].Hits +
                        facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_HitsAsc_LuceneQuery()
        {
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 2, TermSortMode = FacetTermSortMode.HitsAsc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets");

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderBy(x => x.Count).ThenBy(x => x.Manufacturer)
                        .Select(x => x.Manufacturer.ToLower()).ToList();

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(3, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(3, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(camerasByHits[2], facetResults.Results["Manufacturer"].RemainingTerms[0]);
                    Assert.Equal(camerasByHits[3], facetResults.Results["Manufacturer"].RemainingTerms[1]);
                    Assert.Equal(camerasByHits[4], facetResults.Results["Manufacturer"].RemainingTerms[2]);

                    Assert.Equal(_data.Count(),
                        facetResults.Results["Manufacturer"].Values[0].Hits +
                        facetResults.Results["Manufacturer"].Values[1].Hits +
                        facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_HitsDesc_LuceneQuery()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 20, TermSortMode = FacetTermSortMode.HitsDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets");

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).ToList();

                    Assert.Equal(5, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    if (camerasByHits[1] != facetResults.Results["Manufacturer"].Values[1].Range)
                        WaitForUserToContinueTheTest(store, debug: false);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);
                    Assert.Equal(camerasByHits[2], facetResults.Results["Manufacturer"].Values[2].Range);
                    Assert.Equal(camerasByHits[3], facetResults.Results["Manufacturer"].Values[3].Range);
                    Assert.Equal(camerasByHits[4], facetResults.Results["Manufacturer"].Values[4].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(0, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(0, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(0, facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        private void Setup(IDocumentStore store)
        {
            using (var s = store.OpenSession())
            {
                store.Admin.Send(new PutIndexOperation("CameraCost",
                    new IndexDefinition
                    {
                        Maps =
                        {
                          @"from camera in docs 
                            select new 
                            { 
                                camera.Manufacturer, 
                                camera.Model, 
                                camera.Cost,
                                camera.DateOfListing,
                                camera.Megapixels
                            }"
                        }
                    }));

                var counter = 0;
                foreach (var camera in _data)
                {
                    s.Store(camera);
                    counter++;

                    if (counter % (NumCameras / 25) == 0)
                        s.SaveChanges();
                }
                s.SaveChanges();

                WaitForIndexing(store);
            }
        }
    }
}
