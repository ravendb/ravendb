using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.SlowTests.Faceted
{
    public class FacetPaging : FacetTestBase
    {
        private readonly IList<Camera> _data;
        private const int NumCameras = 1000;

        public FacetPaging()
        {
            _data = GetCameras(NumCameras);
        }

        [Fact]
        public void CanPerformFacetedPagingSearchWithNoPageSizeNoMaxResults_HitsDesc()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = null, TermSortMode = FacetTermSortMode.HitsDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Skip(2).Select(x => x.Manufacturer.ToLower()).ToList();

                    Assert.Equal(3, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);
                    Assert.Equal(camerasByHits[2], facetResults.Results["Manufacturer"].Values[2].Range);

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
        public void CanPerformFacetedPagingSearchWithNoPageSizeWithMaxResults_HitsDesc()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 2, TermSortMode = FacetTermSortMode.HitsDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Skip(2).Take(2).Select(x => x.Manufacturer.ToLower()).ToList();

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Last().Count, facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedPagingSearchWithPageSize_HitsDesc()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 3, TermSortMode = FacetTermSortMode.HitsDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2, 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).Skip(2).Take(2).ToList();

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Last().Count, facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedPagingSearchWithPageSize_HitsAsc()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 3, TermSortMode = FacetTermSortMode.HitsAsc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2, 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderBy(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).Skip(2).Take(2).ToList();

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(cameraCounts.OrderBy(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Last().Count, facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedPagingSearchWithPageSize_TermDesc()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 3, TermSortMode = FacetTermSortMode.ValueDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2, 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).Skip(2).Take(2).ToList();

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(cameraCounts.OrderByDescending(x => x.Manufacturer.ToLower()).Last().Count, facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedPagingSearchWithPageSize_TermAsc()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 3, TermSortMode = FacetTermSortMode.ValueAsc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2, 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                           into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderBy(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).Skip(2).Take(2).ToList();

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(cameraCounts.OrderBy(x => x.Manufacturer.ToLower()).Last().Count, facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedPagingSearchWithPageSize_HitsDesc_LuceneQuery()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 3, TermSortMode = FacetTermSortMode.HitsDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2, 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                                             into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).Skip(2).Take(2).ToList();

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Last().Count, facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedPagingSearchWithPageSize_HitsAsc_LuceneQuery()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 3, TermSortMode = FacetTermSortMode.HitsAsc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2, 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                                             into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderBy(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).Skip(2).Take(2).ToList();

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(cameraCounts.OrderBy(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Last().Count, facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedPagingSearchWithPageSize_TermDesc_LuceneQuery()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 3, TermSortMode = FacetTermSortMode.ValueDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2, 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                                             into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).Skip(2).Take(2).ToList();

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(cameraCounts.OrderByDescending(x => x.Manufacturer.ToLower()).Last().Count, facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedPagingSearchWithPageSize_TermAsc_LuceneQuery()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 3, TermSortMode = FacetTermSortMode.ValueAsc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2, 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                                             into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderBy(x => x.Manufacturer.ToLower()).Select(x => x.Manufacturer.ToLower()).Skip(2).Take(2).ToList();

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(cameraCounts.OrderBy(x => x.Manufacturer.ToLower()).Last().Count, facetResults.Results["Manufacturer"].RemainingHits);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedPagingSearchWithNoPageSizeNoMaxResults_HitsDesc_LuceneQuery()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = null, TermSortMode = FacetTermSortMode.HitsDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                                             into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Skip(2).Select(x => x.Manufacturer.ToLower()).ToList();

                    Assert.Equal(3, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);
                    Assert.Equal(camerasByHits[2], facetResults.Results["Manufacturer"].Values[2].Range);

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
        public void CanPerformFacetedPagingSearchWithNoPageSizeWithMaxResults_HitsDesc_LuceneQuery()
        {
            //also specify more results than we have
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 2, TermSortMode = FacetTermSortMode.HitsDesc, IncludeRemainingTerms = true } };

            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Advanced.DocumentQuery<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets", 2);

                    var cameraCounts = from d in _data
                                       group d by d.Manufacturer
                                                             into result
                                       select new { Manufacturer = result.Key, Count = result.Count() };
                    var camerasByHits = cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Skip(2).Take(2).Select(x => x.Manufacturer.ToLower()).ToList();

                    Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
                    Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
                    Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

                    foreach (var facet in facetResults.Results["Manufacturer"].Values)
                    {
                        var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
                        Assert.Equal(inMemoryCount, facet.Hits);
                    }

                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTermsCount);
                    Assert.Equal(1, facetResults.Results["Manufacturer"].RemainingTerms.Count());
                    Assert.Equal(cameraCounts.OrderByDescending(x => x.Count).ThenBy(x => x.Manufacturer.ToLower()).Last().Count, facetResults.Results["Manufacturer"].RemainingHits);
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
