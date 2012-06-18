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
	public class FacetedIndex : RavenTest
	{
		private readonly IList<Camera> _data;
		private readonly List<Facet> _facets;
		private const int NumCameras = 1000;

		public FacetedIndex()
		{
			_data = FacetedIndexTestHelper.GetCameras(NumCameras);

			_facets = new List<Facet>
			          	{
			          		new Facet {Name = "Manufacturer"},
			          		//default is term query		                         
			          		//In Lucene [ is inclusive, { is exclusive
			          		new Facet
			          			{
			          				Name = "Cost_Range",
			          				Mode = FacetMode.Ranges,
			          				Ranges =
			          					{
			          						"[NULL TO Dx200.0]",
			          						"[Dx200.0 TO Dx400.0]",
			          						"[Dx400.0 TO Dx600.0]",
			          						"[Dx600.0 TO Dx800.0]",
			          						"[Dx800.0 TO NULL]",
			          					}
			          			},
			          		new Facet
			          			{
			          				Name = "Megapixels_Range",
			          				Mode = FacetMode.Ranges,
			          				Ranges =
			          					{
			          						"[NULL TO Dx3.0]",
			          						"[Dx3.0 TO Dx7.0]",
			          						"[Dx7.0 TO Dx10.0]",
			          						"[Dx10.0 TO NULL]",
			          					}
			          			}
			          	};
		}

		[Fact]
		public void CanPerformFacetedSearch_Remotely()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				ExecuteTest(store);
			}
		}

		[Fact]
		public void CanPerformFacetedSearch_Remotely_Lazy()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				Setup(store);

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
							.Where(exp)
							.ToFacetsLazy("facets/CameraFacets");

						Assert.Equal(oldRequests, s.Advanced.NumberOfRequests);

						var filteredData = _data.Where(exp.Compile()).ToList();
						CheckFacetResultsMatchInMemoryData(facetResults.Value, filteredData);

						Assert.Equal(oldRequests +1, s.Advanced.NumberOfRequests);

					}
				}
			}
		}

		[Fact]
		public void CanPerformFacetedSearch_Remotely_Lazy_can_work_with_others()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				Setup(store);

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
						var load = s.Advanced.Lazily.Load<Camera>(oldRequests);
						var facetResults = s.Query<Camera>("CameraCost")
							.Where(exp)
							.ToFacetsLazy("facets/CameraFacets");

						Assert.Equal(oldRequests, s.Advanced.NumberOfRequests);

						var filteredData = _data.Where(exp.Compile()).ToList();
						CheckFacetResultsMatchInMemoryData(facetResults.Value, filteredData);
						var forceLoading = load.Value;
						Assert.Equal(oldRequests + 1, s.Advanced.NumberOfRequests);

					}
				}
			}
		}

		[Fact]
		public void CanPerformFacetedSearch_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				ExecuteTest(store);
			}
		}


		private void ExecuteTest(IDocumentStore store)
		{
			Setup(store);

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
						.Where(exp)
						.ToFacets("facets/CameraFacets");
					facetQueryTimer.Stop();

					var filteredData = _data.Where(exp.Compile()).ToList();
					CheckFacetResultsMatchInMemoryData(facetResults, filteredData);
				}
			}
		}

		private void Setup(IDocumentStore store)
		{
			using (var s = store.OpenSession())
			{
				s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = _facets });
				s.SaveChanges();

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

		private void PrintFacetResults(IDictionary<string, IEnumerable<FacetValue>> facetResults)
		{
			foreach (var kvp in facetResults)
			{
				if (kvp.Value.Count() > 0)
				{
					Console.WriteLine(kvp.Key + ":");
					foreach (var facet in kvp.Value)
					{
						Console.WriteLine("    {0}: {1}", facet.Range, facet.Count);
					}
					Console.WriteLine();
				}
			}
		}

		private void CheckFacetResultsMatchInMemoryData(
					IDictionary<string, IEnumerable<FacetValue>> facetResults,
					List<Camera> filteredData)
		{
			Assert.Equal(filteredData.GroupBy(x => x.Manufacturer).Count(),
						facetResults["Manufacturer"].Count());
			foreach (var facet in facetResults["Manufacturer"])
			{
				var inMemoryCount = filteredData.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
				Assert.Equal(inMemoryCount, facet.Count);
			}

			//Go through the expected (in-memory) results and check that there is a corresponding facet result
			//Not the prettiest of code, but it works!!!
			var costFacets = facetResults["Cost_Range"];
			CheckFacetCount(filteredData.Where(x => x.Cost <= 200.0m).Count(),
							costFacets.FirstOrDefault(x => x.Range == "[NULL TO Dx200.0]"));
			CheckFacetCount(filteredData.Where(x => x.Cost >= 200.0m && x.Cost <= 400).Count(),
							costFacets.FirstOrDefault(x => x.Range == "[Dx200.0 TO Dx400.0]"));
			CheckFacetCount(filteredData.Where(x => x.Cost >= 400.0m && x.Cost <= 600.0m).Count(),
							costFacets.FirstOrDefault(x => x.Range == "[Dx400.0 TO Dx600.0]"));
			CheckFacetCount(filteredData.Where(x => x.Cost >= 600.0m && x.Cost <= 800.0m).Count(),
							costFacets.FirstOrDefault(x => x.Range == "[Dx600.0 TO Dx800.0]"));
			CheckFacetCount(filteredData.Where(x => x.Cost >= 800.0m).Count(),
							costFacets.FirstOrDefault(x => x.Range == "[Dx800.0 TO NULL]"));

			//Test the Megapixels_Range facets using the same method
			var megapixelsFacets = facetResults["Megapixels_Range"];
			CheckFacetCount(filteredData.Where(x => x.Megapixels <= 3.0m).Count(),
							megapixelsFacets.FirstOrDefault(x => x.Range == "[NULL TO Dx3.0]"));
			CheckFacetCount(filteredData.Where(x => x.Megapixels >= 3.0m && x.Megapixels <= 7.0m).Count(),
							megapixelsFacets.FirstOrDefault(x => x.Range == "[Dx3.0 TO Dx7.0]"));
			CheckFacetCount(filteredData.Where(x => x.Megapixels >= 7.0m && x.Megapixels <= 10.0m).Count(),
							megapixelsFacets.FirstOrDefault(x => x.Range == "[Dx7.0 TO Dx10.0]"));
			CheckFacetCount(filteredData.Where(x => x.Megapixels >= 10.0m).Count(),
							megapixelsFacets.FirstOrDefault(x => x.Range == "[Dx10.0 TO NULL]"));
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
