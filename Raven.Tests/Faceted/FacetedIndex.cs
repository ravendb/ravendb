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
			          				Name = "Cost",
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
			          				Name = "Megapixels",
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

					if (counter % 1024 == 0)
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
			foreach (var facet in facetResults["Manufacturer"])
			{
				var inMemoryCount = filteredData.Where(x => x.Manufacturer.ToLower() == facet.Range).Count();
				Assert.Equal(inMemoryCount, facet.Count);
				//Console.WriteLine("{0} - Expected {1}, Got {2} {3}",
				//    facet.Range, inMemoryCount, facet.Count, inMemoryCount != facet.Count ? "*****" : "");
			}

			//In Lucene [ is inclusive, { is exclusive
			foreach (var facet in facetResults["Cost"])
			{
				var inMemoryCount = 0;
				switch (facet.Range)
				{
					case "[NULL TO 200.0]":
						inMemoryCount = filteredData.Where(x => x.Cost <= 200.0m).Count();
						break;
					case "[200.0 TO 400.0]":
						inMemoryCount = filteredData.Where(x => x.Cost >= 200.0m && x.Cost <= 400).Count();
						break;
					case "[400.0 TO 600.0]":
						inMemoryCount = filteredData.Where(x => x.Cost >= 400.0m && x.Cost <= 600.0m).Count();
						break;
					case "[600.0 TO 800.0]":
						inMemoryCount = filteredData.Where(x => x.Cost >= 600.0m && x.Cost <= 800.0m).Count();
						break;
					case "[800.0 TO NULL]":
						inMemoryCount = filteredData.Where(x => x.Cost >= 800.0m).Count();
						break;
				}
				Assert.Equal(inMemoryCount, facet.Count);
				//Console.WriteLine("{0} - Expected {1}, Got {2} {3}",
				//    facet.Range, inMemoryCount, facet.Count, inMemoryCount != facet.Count ? "*****" : "");
			}

			//In Lucene [ is inclusive, { is exclusive
			foreach (var facet in facetResults["Megapixels"])
			{
				var inMemoryCount = 0;
				switch (facet.Range)
				{
					case "[NULL TO 3.0]":
						inMemoryCount = filteredData.Where(x => x.Megapixels <= 3.0m).Count();
						break;
					case "[3.0 TO 7.0]":
						inMemoryCount = filteredData.Where(x => x.Megapixels >= 3.0m && x.Megapixels <= 7.0m).Count();
						break;
					case "[7.0 TO 10.0]":
						inMemoryCount = filteredData.Where(x => x.Megapixels >= 7.0m && x.Megapixels <= 10.0m).Count();
						break;
					case "[10.0 TO NULL]":
						inMemoryCount = filteredData.Where(x => x.Megapixels >= 10.0m).Count();
						break;
				}
				Assert.Equal(inMemoryCount, facet.Count);
				//Console.WriteLine("{0} - Expected {1}, Got {2} {3}", 
				//    facet.Range, inMemoryCount, facet.Count, inMemoryCount != facet.Count ? "*****" : "");
			}
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
