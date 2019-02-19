//-----------------------------------------------------------------------
// <copyright file="FacetedIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Facets;
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
            var facets = new List<Facet> { new Facet { FieldName = "Manufacturer" }, new Facet { FieldName = "Model" } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .AggregateUsing("facets/CameraFacets")
                        .Execute();

                    Assert.Equal(5, facetResults["Manufacturer"].Values.Count());
                    Assert.Equal("canon", facetResults["Manufacturer"].Values[0].Range);
                    Assert.Equal("jessops", facetResults["Manufacturer"].Values[1].Range);
                    Assert.Equal("nikon", facetResults["Manufacturer"].Values[2].Range);
                    Assert.Equal("phillips", facetResults["Manufacturer"].Values[3].Range);
                    Assert.Equal("sony", facetResults["Manufacturer"].Values[4].Range);

                    foreach (var facet in facetResults["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

                    Assert.Equal(new List<string>(), facetResults["Manufacturer"].RemainingTerms);
                    Assert.Equal(0, facetResults["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(0, facetResults["Manufacturer"].RemainingHits);

                    Assert.Equal(5, facetResults["Model"].Values.Count());
                    Assert.Equal("model1", facetResults["Model"].Values[0].Range);
                    Assert.Equal("model2", facetResults["Model"].Values[1].Range);
                    Assert.Equal("model3", facetResults["Model"].Values[2].Range);
                    Assert.Equal("model4", facetResults["Model"].Values[3].Range);
                    Assert.Equal("model5", facetResults["Model"].Values[4].Range);

                    foreach (var facet in facetResults["Model"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Model.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

                    Assert.Equal(new List<string>(), facetResults["Model"].RemainingTerms);
                    Assert.Equal(0, facetResults["Model"].RemainingTermsCount);
                    Assert.Equal(0, facetResults["Model"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_TermAsc()
        {
            var facets = new List<Facet>
            {
                new Facet
                {
                    FieldName = "Manufacturer",
                    Options = new FacetOptions
                    {
                        PageSize = 2,
                        //MaxResults = 2,
                        IncludeRemainingTerms = true
                    }
                }
            };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .Where(x => x.DateOfListing > new DateTime(2000, 1, 1))
                        .AggregateUsing("facets/CameraFacets")
                        .Execute();

                    Assert.Equal(2, facetResults["Manufacturer"].Values.Count());
                    Assert.Equal("canon", facetResults["Manufacturer"].Values[0].Range);
                    Assert.Equal("jessops", facetResults["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.DateOfListing > new DateTime(2000, 1, 1)).Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

                    Assert.Equal(3, facetResults["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(3, facetResults["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal("nikon", facetResults["Manufacturer"].RemainingTerms[0]);
                    Assert.Equal("phillips", facetResults["Manufacturer"].RemainingTerms[1]);
                    Assert.Equal("sony", facetResults["Manufacturer"].RemainingTerms[2]);

                    Assert.Equal(_data.Count(x => x.DateOfListing > new DateTime(2000, 1, 1)),
                        facetResults["Manufacturer"].Values[0].Count +
                        facetResults["Manufacturer"].Values[1].Count +
                        facetResults["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_TermDesc()
        {
            var facets = new List<Facet>
            {
                new Facet
                {
                    FieldName = "Manufacturer",
                    Options = new FacetOptions
                    {
                        PageSize = 3,
                        //MaxResults = 3,
                        TermSortMode = FacetTermSortMode.ValueDesc,
                        IncludeRemainingTerms = true
                    }
                }
            };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .Where(x => x.DateOfListing > new DateTime(2000, 1, 1))
                        .AggregateUsing("facets/CameraFacets")
                        .Execute();

                    Assert.Equal(3, facetResults["Manufacturer"].Values.Count());
                    Assert.Equal("sony", facetResults["Manufacturer"].Values[0].Range);
                    Assert.Equal("phillips", facetResults["Manufacturer"].Values[1].Range);
                    Assert.Equal("nikon", facetResults["Manufacturer"].Values[2].Range);

                    foreach (var facet in facetResults["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.DateOfListing > new DateTime(2000, 1, 1)).Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

                    Assert.Equal(2, facetResults["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(2, facetResults["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal("jessops", facetResults["Manufacturer"].RemainingTerms[0]);
                    Assert.Equal("canon", facetResults["Manufacturer"].RemainingTerms[1]);

                    Assert.Equal(_data.Count(x => x.DateOfListing > new DateTime(2000, 1, 1)),
                        facetResults["Manufacturer"].Values[0].Count +
                        facetResults["Manufacturer"].Values[1].Count +
                        facetResults["Manufacturer"].Values[2].Count +
                        facetResults["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_HitsAsc()
        {
            var facets = new List<Facet>
            {
                new Facet
                {
                    FieldName = "Manufacturer",
                    Options = new FacetOptions
                    {
                        PageSize = 2,
                        //MaxResults = 2,
                        TermSortMode = FacetTermSortMode.CountAsc,
                        IncludeRemainingTerms = true
                    }
                }
            };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .AggregateUsing("facets/CameraFacets")
                        .Execute();

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderBy(x => x.Count)
                        .ThenBy(x => x.Manufacturer).Select(x => x.Manufacturer.ToLower()).ToList();

                    var manufacturer = facetResults["Manufacturer"];
                    Assert.Equal(2, manufacturer.Values.Count());
                    Assert.Equal(camerasByHits[0], manufacturer.Values[0].Range);
                    Assert.Equal(camerasByHits[1], manufacturer.Values[1].Range);

                    foreach (var facet in manufacturer.Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

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
                        manufacturer.Values[0].Count +
                        manufacturer.Values[1].Count +
                        manufacturer.RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_HitsDesc()
        {
            //also specify more results than we have
            var facets = new List<Facet>
            {
                new Facet
                {
                    FieldName = "Manufacturer",
                    Options = new FacetOptions
                    {
                        PageSize = 20,
                        //MaxResults = 20,
                        TermSortMode = FacetTermSortMode.CountDesc,
                        IncludeRemainingTerms = true
                    }
                }
            };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .AggregateUsing("facets/CameraFacets")
                        .Execute();

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).ToList();

                    Assert.Equal(5, facetResults["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults["Manufacturer"].Values[0].Range);
                    if (camerasByHits[1] != facetResults["Manufacturer"].Values[1].Range)
                        WaitForUserToContinueTheTest(store, debug: false);
                    Assert.Equal(camerasByHits[1], facetResults["Manufacturer"].Values[1].Range);
                    Assert.Equal(camerasByHits[2], facetResults["Manufacturer"].Values[2].Range);
                    Assert.Equal(camerasByHits[3], facetResults["Manufacturer"].Values[3].Range);
                    Assert.Equal(camerasByHits[4], facetResults["Manufacturer"].Values[4].Range);

                    foreach (var facet in facetResults["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

                    Assert.Equal(0, facetResults["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(0, facetResults["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(0, facetResults["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformSearchWithTwoDefaultFacets_LuceneQuery()
        {
            var facets = new List<Facet> { new Facet { FieldName = "Manufacturer" }, new Facet { FieldName = "Model" } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .AggregateUsing("facets/CameraFacets")
                        .Execute();

                    Assert.Equal(5, facetResults["Manufacturer"].Values.Count());
                    Assert.Equal("canon", facetResults["Manufacturer"].Values[0].Range);
                    Assert.Equal("jessops", facetResults["Manufacturer"].Values[1].Range);
                    Assert.Equal("nikon", facetResults["Manufacturer"].Values[2].Range);
                    Assert.Equal("phillips", facetResults["Manufacturer"].Values[3].Range);
                    Assert.Equal("sony", facetResults["Manufacturer"].Values[4].Range);

                    foreach (var facet in facetResults["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

                    Assert.Equal(new List<string>(), facetResults["Manufacturer"].RemainingTerms);
                    Assert.Equal(0, facetResults["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(0, facetResults["Manufacturer"].RemainingHits);

                    Assert.Equal(5, facetResults["Model"].Values.Count());
                    Assert.Equal("model1", facetResults["Model"].Values[0].Range);
                    Assert.Equal("model2", facetResults["Model"].Values[1].Range);
                    Assert.Equal("model3", facetResults["Model"].Values[2].Range);
                    Assert.Equal("model4", facetResults["Model"].Values[3].Range);
                    Assert.Equal("model5", facetResults["Model"].Values[4].Range);

                    foreach (var facet in facetResults["Model"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Model.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

                    Assert.Equal(new List<string>(), facetResults["Model"].RemainingTerms);
                    Assert.Equal(0, facetResults["Model"].RemainingTermsCount);
                    Assert.Equal(0, facetResults["Model"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_TermAsc_LuceneQuery()
        {
            var facets = new List<Facet>
            {
                new Facet
                {
                    FieldName = "Manufacturer",
                    Options = new FacetOptions
                    {
                        PageSize = 2,
                        //MaxResults = 2,
                        IncludeRemainingTerms = true
                    }
                }
            };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .WhereGreaterThan(x => x.DateOfListing, new DateTime(2000, 1, 1))
                        .Take(2)
                        .AggregateUsing("facets/CameraFacets")
                        .Execute();

                    Assert.Equal(2, facetResults["Manufacturer"].Values.Count());
                    Assert.Equal("canon", facetResults["Manufacturer"].Values[0].Range);
                    Assert.Equal("jessops", facetResults["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.DateOfListing > new DateTime(2000, 1, 1)).Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

                    Assert.Equal(3, facetResults["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(3, facetResults["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal("nikon", facetResults["Manufacturer"].RemainingTerms[0]);
                    Assert.Equal("phillips", facetResults["Manufacturer"].RemainingTerms[1]);
                    Assert.Equal("sony", facetResults["Manufacturer"].RemainingTerms[2]);

                    Assert.Equal(_data.Count(x => x.DateOfListing > new DateTime(2000, 1, 1)),
                        facetResults["Manufacturer"].Values[0].Count +
                        facetResults["Manufacturer"].Values[1].Count +
                        facetResults["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_TermDesc_LuceneQuery()
        {
            var facets = new List<Facet>
            {
                new Facet
                {
                    FieldName = "Manufacturer",
                    Options = new FacetOptions
                    {
                        PageSize = 3,
                        //MaxResults = 3,
                        TermSortMode = FacetTermSortMode.ValueDesc,
                        IncludeRemainingTerms = true
                    }
                }
            };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .WhereGreaterThan(x => x.DateOfListing, new DateTime(2000, 1, 1))
                        .AggregateUsing("facets/CameraFacets")
                        .Execute();

                    Assert.Equal(3, facetResults["Manufacturer"].Values.Count());
                    Assert.Equal("sony", facetResults["Manufacturer"].Values[0].Range);
                    Assert.Equal("phillips", facetResults["Manufacturer"].Values[1].Range);
                    Assert.Equal("nikon", facetResults["Manufacturer"].Values[2].Range);

                    foreach (var facet in facetResults["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.DateOfListing > new DateTime(2000, 1, 1)).Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

                    Assert.Equal(2, facetResults["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(2, facetResults["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal("jessops", facetResults["Manufacturer"].RemainingTerms[0]);
                    Assert.Equal("canon", facetResults["Manufacturer"].RemainingTerms[1]);

                    Assert.Equal(_data.Count(x => x.DateOfListing > new DateTime(2000, 1, 1)),
                        facetResults["Manufacturer"].Values[0].Count +
                        facetResults["Manufacturer"].Values[1].Count +
                        facetResults["Manufacturer"].Values[2].Count +
                        facetResults["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_HitsAsc_LuceneQuery()
        {
            var facets = new List<Facet>
            {
                new Facet
                {
                    FieldName = "Manufacturer",
                    Options = new FacetOptions
                    {
                        PageSize = 2,
                        //MaxResults = 2,
                        TermSortMode = FacetTermSortMode.CountAsc,
                        IncludeRemainingTerms = true
                    }
                }
            };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .AggregateUsing("facets/CameraFacets")
                        .Execute();

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderBy(x => x.Count).ThenBy(x => x.Manufacturer)
                        .Select(x => x.Manufacturer.ToLower()).ToList();

                    Assert.Equal(2, facetResults["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

                    Assert.Equal(3, facetResults["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(3, facetResults["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(camerasByHits[2], facetResults["Manufacturer"].RemainingTerms[0]);
                    Assert.Equal(camerasByHits[3], facetResults["Manufacturer"].RemainingTerms[1]);
                    Assert.Equal(camerasByHits[4], facetResults["Manufacturer"].RemainingTerms[2]);

                    Assert.Equal(_data.Count(),
                        facetResults["Manufacturer"].Values[0].Count +
                        facetResults["Manufacturer"].Values[1].Count +
                        facetResults["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedLimitSearch_HitsDesc_LuceneQuery()
        {
            //also specify more results than we have
            var facets = new List<Facet>
            {
                new Facet
                {
                    FieldName = "Manufacturer",
                    Options = new FacetOptions
                    {
                        PageSize = 20,
                        //MaxResults = 20,
                        TermSortMode = FacetTermSortMode.CountDesc,
                        IncludeRemainingTerms = true
                    }
                }
            };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .AggregateUsing("facets/CameraFacets")
                        .Execute();

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).ToList();

                    Assert.Equal(5, facetResults["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults["Manufacturer"].Values[0].Range);
                    if (camerasByHits[1] != facetResults["Manufacturer"].Values[1].Range)
                        WaitForUserToContinueTheTest(store, debug: false);
                    Assert.Equal(camerasByHits[1], facetResults["Manufacturer"].Values[1].Range);
                    Assert.Equal(camerasByHits[2], facetResults["Manufacturer"].Values[2].Range);
                    Assert.Equal(camerasByHits[3], facetResults["Manufacturer"].Values[3].Range);
                    Assert.Equal(camerasByHits[4], facetResults["Manufacturer"].Values[4].Range);

                    foreach (var facet in facetResults["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Count);
                    }

                    Assert.Equal(0, facetResults["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(0, facetResults["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(0, facetResults["Manufacturer"].RemainingHits);
                }
            }
        }

        private void Setup(IDocumentStore store)
        {
            using (var s = store.OpenSession())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Name = "CameraCost",
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
                    }
            }))
            ;

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
