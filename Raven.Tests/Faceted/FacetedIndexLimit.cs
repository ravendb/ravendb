//-----------------------------------------------------------------------
// <copyright file="FacetedIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Linq;
using Xunit;
using Raven.Abstractions.Indexing;
using System.Threading;
using System.Linq.Expressions;
using Raven.Client.Document;

namespace Raven.Tests.Faceted
{
	public class FacetedIndexLimit : RavenTest
	{
		private readonly IList<Camera> _data;
		private const int NumCameras = 1000;

		public FacetedIndexLimit()
		{
			_data = FacetedIndexTestHelper.GetCameras(NumCameras);
		}

		[Fact]
		public void CanPerformFacetedLimitSearch_TermAsc()
		{
			var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 2 } };

			using (var store = NewDocumentStore())
			{
				Setup(store);

				using(var s = store.OpenSession())
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
						Assert.Equal(inMemoryCount, facet.Count);
					}

					Assert.Equal(5, facetResults.Results["Manufacturer"].Terms.Count());
					Assert.Equal("canon", facetResults.Results["Manufacturer"].Terms[0]);
					Assert.Equal("jessops", facetResults.Results["Manufacturer"].Terms[1]);
					Assert.Equal("nikon", facetResults.Results["Manufacturer"].Terms[2]);
					Assert.Equal("phillips", facetResults.Results["Manufacturer"].Terms[3]);
					Assert.Equal("sony", facetResults.Results["Manufacturer"].Terms[4]);
				}
			}
		}

		[Fact]
		public void CanPerformFacetedLimitSearch_TermDesc()
		{
			var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 3, TermSortMode = FacetTermSortMode.ValueDesc } };

			using (var store = NewDocumentStore())
			{
				Setup(store);

				using(var s = store.OpenSession())
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
						Assert.Equal(inMemoryCount, facet.Count);
					}

					Assert.Equal(5, facetResults.Results["Manufacturer"].Terms.Count());
					Assert.Equal("sony", facetResults.Results["Manufacturer"].Terms[0]);
					Assert.Equal("phillips", facetResults.Results["Manufacturer"].Terms[1]);
					Assert.Equal("nikon", facetResults.Results["Manufacturer"].Terms[2]);
					Assert.Equal("jessops", facetResults.Results["Manufacturer"].Terms[3]);
					Assert.Equal("canon", facetResults.Results["Manufacturer"].Terms[4]);
				}
			}
		}

		[Fact]
		public void CanPerformFacetedLimitSearch_HitsAsc()
		{
			var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 2, TermSortMode = FacetTermSortMode.HitsAsc } };

			using (var store = NewDocumentStore())
			{
				Setup(store);

				using(var s = store.OpenSession())
				{
					s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
					s.SaveChanges();

					var facetResults = s.Query<Camera>("CameraCost")
						.ToFacets("facets/CameraFacets");

					var cameraCounts = from d in _data
					                  group d by d.Manufacturer
					                  into result
					                  select new {Manufacturer = result.Key, Count = result.Count()};
					var camerasByHits = cameraCounts.OrderBy(x => x.Count).Select(x => x.Manufacturer.ToLower()).ToList();

					Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count());
					Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
					Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);

					foreach (var facet in facetResults.Results["Manufacturer"].Values)
					{
						var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
						Assert.Equal(inMemoryCount, facet.Count);
					}

					Assert.Equal(5, facetResults.Results["Manufacturer"].Terms.Count());
					Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Terms[0]);
					Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Terms[1]);
					Assert.Equal(camerasByHits[2], facetResults.Results["Manufacturer"].Terms[2]);
					Assert.Equal(camerasByHits[3], facetResults.Results["Manufacturer"].Terms[3]);
					Assert.Equal(camerasByHits[4], facetResults.Results["Manufacturer"].Terms[4]);
				}
			}
		}

		[Fact]
		public void CanPerformFacetedLimitSearch_HitsDesc()
		{
			//also specify more results than we have
			var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 20, TermSortMode = FacetTermSortMode.HitsDesc } };

			using (var store = NewDocumentStore())
			{
				Setup(store);

				using(var s = store.OpenSession())
				{
					s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
					s.SaveChanges();

					var facetResults = s.Query<Camera>("CameraCost")
						.ToFacets("facets/CameraFacets");

					var cameraCounts = from d in _data
					                  group d by d.Manufacturer
					                  into result
					                  select new {Manufacturer = result.Key, Count = result.Count()};
					var camerasByHits = cameraCounts.OrderByDescending(x => x.Count).Select(x => x.Manufacturer.ToLower()).ToList();

					Assert.Equal(5, facetResults.Results["Manufacturer"].Values.Count());
					Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Values[0].Range);
					Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Values[1].Range);
					Assert.Equal(camerasByHits[2], facetResults.Results["Manufacturer"].Values[2].Range);
					Assert.Equal(camerasByHits[3], facetResults.Results["Manufacturer"].Values[3].Range);
					Assert.Equal(camerasByHits[4], facetResults.Results["Manufacturer"].Values[4].Range);

					foreach (var facet in facetResults.Results["Manufacturer"].Values)
					{
						var inMemoryCount = _data.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
						Assert.Equal(inMemoryCount, facet.Count);
					}

					Assert.Equal(5, facetResults.Results["Manufacturer"].Terms.Count());
					Assert.Equal(camerasByHits[0], facetResults.Results["Manufacturer"].Terms[0]);
					Assert.Equal(camerasByHits[1], facetResults.Results["Manufacturer"].Terms[1]);
					Assert.Equal(camerasByHits[2], facetResults.Results["Manufacturer"].Terms[2]);
					Assert.Equal(camerasByHits[3], facetResults.Results["Manufacturer"].Terms[3]);
					Assert.Equal(camerasByHits[4], facetResults.Results["Manufacturer"].Terms[4]);
				}
			}
		}

		private void Setup(IDocumentStore store)
		{
			using (var s = store.OpenSession())
			{
				store.DatabaseCommands.PutIndex("CameraCost",
												new IndexDefinition
												{
													Map =
														@"from camera in docs 
                                                        select new 
                                                        { 
                                                            camera.Manufacturer, 
                                                            camera.Model, 
                                                            camera.Cost,
                                                            camera.DateOfListing,
                                                            camera.Megapixels
                                                        }"
												});

				var counter = 0;
				foreach (var camera in _data)
				{
					s.Store(camera);
					counter++;

					if (counter % (NumCameras / 25) == 0)
						s.SaveChanges();
				}
				s.SaveChanges();

				s.Query<Camera>("CameraCost")
					.Customize(x => x.WaitForNonStaleResults())
					.ToList();
			}
		}
	}
}
