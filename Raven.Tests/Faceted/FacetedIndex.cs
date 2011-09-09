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
				Url = "http://localhost:8080"
			}.Initialize())
			{
				ExecuteTest(store);
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
			using (var s = store.OpenSession())
			{
				s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = _facets });
				s.SaveChanges();

				store.DatabaseCommands.PutIndex("CameraCost",
				                                new IndexDefinition
				                                {
				                                	Map = @"from camera in docs 
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

				//WaitForUserToContinueTheTest(store);

				var expressions = new Expression<Func<Camera, bool>>[]
				{
					x => x.Cost >= 100 && x.Cost <= 300,
					x => x.DateOfListing > new DateTime(2000, 1, 1),
					x => x.Megapixels > 5.0m && x.Cost < 500
				};

				foreach (var exp in expressions)
				{
					Console.WriteLine("Query: " + exp);

					var facetQueryTimer = Stopwatch.StartNew();
					var facetResults = s.Query<Camera>("CameraCost")
						.Where(exp)
						.ToFacets("facets/CameraFacets");
					facetQueryTimer.Stop();

					Console.WriteLine("Took {0:0.00} msecs", facetQueryTimer.ElapsedMilliseconds);
					PrintFacetResults(facetResults);

					var filteredData = _data.Where(exp.Compile()).ToList();
					CheckFacetResultsMatchInMemoryData(facetResults, filteredData);
				}
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
            var count1 = filteredData.Where(x => x.Cost <= 200.0m).Count();
            if (count1 > 0)
                Assert.Equal(count1, costFacets.First(x => x.Range == "[NULL TO Dx200.0]").Count);

            var count2 = filteredData.Where(x => x.Cost >= 200.0m && x.Cost <= 400).Count();
            if (count2 > 0)
                Assert.Equal(count2, costFacets.First(x => x.Range == "[Dx200.0 TO Dx400.0]").Count);

            var count3 = filteredData.Where(x => x.Cost >= 400.0m && x.Cost <= 600.0m).Count();
            if (count3 > 0)
                Assert.Equal(count3, costFacets.First(x => x.Range == "[Dx400.0 TO Dx600.0]").Count);

            var count4 = filteredData.Where(x => x.Cost >= 600.0m && x.Cost <= 800.0m).Count();
            if (count4 > 0)
                Assert.Equal(count4, costFacets.First(x => x.Range == "[Dx600.0 TO Dx800.0]").Count);

            var count5 = filteredData.Where(x => x.Cost >= 800.0m).Count();
            if (count5 > 0)
                Assert.Equal(count5, costFacets.First(x => x.Range == "[Dx800.0 TO NULL]").Count);

            //Test the Megapixels_Range facets using the same method
            var megapixelsFacets = facetResults["Megapixels_Range"];
            var count6 = filteredData.Where(x => x.Megapixels <= 3.0m).Count();
            if (count6 > 0)
                Assert.Equal(count6, megapixelsFacets.First(x => x.Range == "[NULL TO Dx3.0]").Count);

            var count7 = filteredData.Where(x => x.Megapixels >= 3.0m && x.Megapixels <= 7.0m).Count();
            if (count7 > 0)
                Assert.Equal(count7, megapixelsFacets.First(x => x.Range == "[Dx3.0 TO Dx7.0]").Count);

            var count8 = filteredData.Where(x => x.Megapixels >= 7.0m && x.Megapixels <= 10.0m).Count();
            if (count8 > 0)
                Assert.Equal(count8, megapixelsFacets.First(x => x.Range == "[Dx7.0 TO Dx10.0]").Count);

            var count9 = filteredData.Where(x => x.Megapixels >= 10.0m).Count();
            if (count9 > 0)
                Assert.Equal(count9, megapixelsFacets.First(x => x.Range == "[Dx10.0 TO NULL]").Count);            
		}

		private static void Log(string text, params object[] args)
		{
			Trace.WriteLine(String.Format(text, args));
			Console.WriteLine(text, args);
		}

		private static double TimeIt(Action action)
		{
			var timer = Stopwatch.StartNew();
			action();
			timer.Stop();
			return timer.ElapsedMilliseconds;
		}
	}
}
