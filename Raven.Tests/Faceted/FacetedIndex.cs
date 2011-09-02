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
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Faceted
{
	//Eventually move this into LinqExtensions.cs in "Raven.Client.Lightweight\Linq"
	public static class Extensions
	{
		public static IDictionary<string, IEnumerable<FacetValue>> ToFacets<T>(this IQueryable<T> queryable, string facetDoc)
		{            
			var ravenQueryInspector = ((RavenQueryInspector<T>)queryable);
			var query = ravenQueryInspector.ToString();
			var provider = queryable.Provider as IRavenQueryProvider;

		    return ravenQueryInspector.DatabaseCommands.GetFacets(ravenQueryInspector.IndexQueried,
		                                                          new IndexQuery {Query = query}, facetDoc);
		}
	}  

	public class FacetedIndex : LocalClientTest
	{		
		private readonly IList<Camera> _data;
		private const int NumCameras = 3000; //500000; //100000

		public FacetedIndex()
		{			
			_data = FacetedIndexTestHelper.GetCameras(NumCameras);

			var facets = new List<Facet>
							 {
								 new Facet {Name = "Manufacturer"}, //default is term query		                         
								 //In Lucene [ is inclusive, { is exclusive
								 new Facet { Name = "Cost",
											Ranges = {
												 "[NULL TO 200.0}",
												 "[200.0 TO 400.0}",
												 "[400.0 TO 600.0}",
												 "[600.0 TO 800.0}",
												 "[800.0 TO NULL]",
											 }
									 },
								 new Facet { Name = "Megapixels",		                                 
										 Ranges =
											 {
												 "[NULL TO 3.0}",
												 "[3.0 TO 7.0}",
												 "[7.0 TO 10.0}",
												 "[10.0 TO NULL]",
											 }
									 }
							 };

			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new FacetSetup {Id = "facets/CameraFacets", Facets = facets});
					s.SaveChanges();

					var counter = 0;
					var setupTime = TimeIt(() =>
											   {
												   foreach (var camera in _data)
												   {
													   s.Store(camera);
													   counter++;

													   if (counter%1024 == 0)
														   s.SaveChanges();
												   }
											   });                    
					Log("Took {0:0.00} secs to setup {1} items in RavenDB, {2:0.00} docs per/sec\n",
							setupTime / 1000.0, NumCameras.ToString("N0"), NumCameras / (setupTime / 1000.0));

					//WaitForUserToContinueTheTest(store);

					var facetResults = s.Query<Camera>()
						.Where(x => x.Cost >= 100 && x.Cost <= 300)
						.ToFacets("CameraFacets");
					foreach (var facet in facetResults["Manufacturer"])
					{
						Console.WriteLine("{0}: {1}", facet.Range, facet.Count);
					}
				}
			}		    								  			

			//DisplayFacetDocInfo(_db);                       

			//var testFacetedQuery = new FacetedIndexQuery
			//                            {
			//                                Query = "Cost_Range:[Dx100.0 TO Dx300.0]",
			//                                Facets = new List<string> { "Manufacturer", "Cost", "Megapixels" },                
			//                            };
			////var qrlString = testFacetedQuery.GetIndexQueryUrl("localhost:8080", "cameraInfo", "indexes");
			//var manufacturerFacets = _data.Where(x => x.Cost >= 100.0m && x.Cost <= 300.0m)
			//                                .GroupBy(x => x.Manufacturer);
			//Log("In-memory LINQ facets:");
			//Array.ForEach(manufacturerFacets.ToArray(), x => Log("\t{0} - {1}", x.Key , x.Count()));            

			//Log("Issuing faceted query..");
			//QueryResult result = WaitForQueryToComplete("cameraInfo", testFacetedQuery);
			//Log("Facet results:");
			//Array.ForEach(result.Facets.ToArray(), facet => Log("\t" + facet.ToString()));            
						
			////QueryResult tempResult = WaitForQueryToComplete("advancedFeatures", new FacetedIndexQuery { Query = "" });          
			////tempResult.Facets.ForEach(x => Log(x.ToString()));
		}

		[Fact]
		public void CanPerformFacetedSearch()
		{

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
