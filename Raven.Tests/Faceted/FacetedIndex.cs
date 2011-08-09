//-----------------------------------------------------------------------
// <copyright file="FacetedIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using Lucene.Net.Util;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Storage;
using Xunit;
using Raven.Abstractions.Exceptions;

namespace Raven.Tests.Faceted
{
	public class FacetedIndex : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase _db;
        private readonly IList<Camera> _data;
        private const int NumCameras = 3000; //500000; //100000

        public FacetedIndex()
        {
            const string dataFolderName = "FacetedSearch";
            var folder = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            IOExtensions.DeleteDirectory(Path.Combine(folder ?? String.Empty, "FacetedSearch"));

            _db = new DocumentDatabase(new RavenConfiguration
                                           {
                                               DataDirectory = @"~\" + dataFolderName,
                                               RunInMemory = false,                                               
                                           });
            _db.SpinBackgroundWorkers();

            _data = FacetedIndexTestHelper.GetCameras(NumCameras);

            var facetSetup = new FacetSetup();
            facetSetup.SetFacetsForIndex("cameraInfo", new List<FacetMode>
                                {
                                    new FacetMode {Name = "Manufacturer"}, //default is term query
                                    //In Lucene [ is inclusive, { is exclusive
                                    new FacetMode {Name = "Cost", DataType = TypeCode.Decimal, 
                                                   Ranges = { 
                                                                "[NULL TO 200.0}", 
                                                                "[200.0 TO 400.0}", 
                                                                "[400.0 TO 600.0}", 
                                                                "[600.0 TO 800.0}", 
                                                                "[800.0 TO NULL]",                                                                 
                                                            }},
                                    new FacetMode {Name = "Megapixels", DataType = TypeCode.Decimal, 
                                                   Ranges = { 
                                                                "[NULL TO 3.0}", 
                                                                "[3.0 TO 7.0}", 
                                                                "[7.0 TO 10.0}", 
                                                                "[10.0 TO NULL]",                                                                
                                                            }}
                                });            
            _db.Put(FacetSetup.FacetSetupDocKey, null, 
                    RavenJObject.FromObject(facetSetup), new RavenJObject(), null);            
                                  
            var setupTime = TimeIt(() => ImportDataIntoRaven());
            Log("Took {0:0.00} secs to setup {1} items in RavenDB, {2:0.00} docs per/sec\n",
                setupTime / 1000.0, NumCameras.ToString("N0"), NumCameras / (setupTime / 1000.0));

            //SanityCheckData();

            var indexSetupTime = TimeIt(() => SetupIndex());
            Log("Took {0:0.00} secs to create index and wait for it to complete, {1:0.00} doc per/sec\n",
                indexSetupTime / 1000.0, NumCameras / (indexSetupTime / 1000.0));
            
            WaitForBackgroundQueueToComplete();

            DisplayFacetDocInfo(_db);                       

            var testFacetedQuery = new FacetedIndexQuery
                                        {
                                            Query = "Cost_Range:[Dx100.0 TO Dx300.0]",
                                            Facets = new List<string> { "Manufacturer", "Cost", "Megapixels" },                
                                        };
            //var qrlString = testFacetedQuery.GetIndexQueryUrl("localhost:8080", "cameraInfo", "indexes");
            var manufacturerFacets = _data.Where(x => x.Cost >= 100.0m && x.Cost <= 300.0m)
                                            .GroupBy(x => x.Manufacturer);
            Log("In-memory LINQ facets:");
            Array.ForEach(manufacturerFacets.ToArray(), x => Log("\t{0} - {1}", x.Key , x.Count()));            

            Log("Issuing faceted query..");
            QueryResult result = WaitForQueryToComplete("cameraInfo", testFacetedQuery);
            Log("Facet results:");
            Array.ForEach(result.Facets.ToArray(), facet => Log("\t" + facet.ToString()));            
                        
            //QueryResult tempResult = WaitForQueryToComplete("advancedFeatures", new FacetedIndexQuery { Query = "" });          
            //tempResult.Facets.ForEach(x => Log(x.ToString()));
        }

        [Fact]
        public void CanPerformFacetedSearch()
        {

        }

	    private void ImportDataIntoRaven()
	    {
	        const int batchSize = 1024;
	        var cmds = new List<ICommandData>(batchSize);
	        for (int i = 0; i < _data.Count; i++)
	        {
	            cmds.Add(new PutCommandData
	                         {
	                             Document = RavenJObject.FromObject(_data[i]),
	                             Metadata = RavenJObject.Parse("{'Raven-Entity-Name': 'Camera'}"),
	                             Key = (i + 1).ToString()
	                         });

	            if (i % batchSize == (batchSize - 1))
	            {
	                _db.Batch(cmds);
	                cmds.Clear();
	            }

	            if (i % 10000 == 0 && i > 0)
	                Log("Wrote {0} records", i.ToString("N0"));
	        }
	        //Make sure we write out all the remaining commands
	        _db.Batch(cmds);
	        cmds.Clear();
	    }

	    private void WaitForBackgroundQueueToComplete()
	    {
	        int outstandingTasks = 0;
	        do
	        {                
	            const string queueName = "HibernatingRhinos/FacetedTrigger/IndexUpdate";
	            try
	            {
	                _db.TransactionalStorage.Batch(action =>
	                                                   {
	                                                       outstandingTasks =
	                                                           action.Queue.PeekFromQueue(queueName).Take(10).Count();
	                                                   });
	            }
	            catch (ConcurrencyException cEx)
	            {
	                Log(cEx.Message);                   
	            }
               
	            if (outstandingTasks > 0)
	            {
	                Thread.Sleep(2000);
	                Log(DateTime.Now.ToLongTimeString() + " There are still items in the queue, sleeping");
	            }
	        } while (outstandingTasks > 0);
	        Log("There are no items left in the queue, continuing");
	    }

	    private void SetupIndex()
        {
            var indexDefinition = new IndexDefinition
            {
                Map = @"from doc in docs 
                            where doc[""@metadata""][""Raven-Entity-Name""] == ""Camera""
                            select new { doc.Id, doc.Manufacturer, doc.Cost, doc.Megapixels }",
            };

            _db.PutIndex("cameraInfo", indexDefinition);

            //Wait for a blank query to complete, so we know the index is not stale
            WaitForQueryToComplete("cameraInfo", new IndexQuery { Query = "" });

            var selectManyIndexDefinition = new IndexDefinition
            {
                Map = @"from doc in docs 
                            where doc[""@metadata""][""Raven-Entity-Name""] == ""Camera""
                            from feature in doc.AdvancedFeatures
                            select new { feature }",
            };

            _db.PutIndex("advancedFeatures", selectManyIndexDefinition);
        }

        private static void DisplayFacetDocInfo(DocumentDatabase database)
        {
            try
            {
                JsonDocument loadedDoc = null;
                loadedDoc = database.Get(FacetSetup.FacetTermDocKey, null);
                if (loadedDoc == null)
                    return;

                var loadedLookup = loadedDoc.DataAsJson.JsonDeserialization<Dictionary<string, Dictionary<string, long[]>>>();
                Log("\n*** FACET TERMS DOC ***");
                foreach (var item in loadedLookup)
                {
                    Log(item.Key + ": ");
                    foreach (var value in item.Value)
                    {
                        var bitset = new OpenBitSet(value.Value, value.Value.Length);
                        if (bitset.Cardinality() > 0)
                        {
                            Log("  \"{0}\" {1} items ", value.Key, bitset.Cardinality());
                        }
                    }
                }
                Log("***********************\n");
            }
            catch (Exception ex)
            {
                Log(ex.Message);
            }
        }
		
		public override void Dispose()
		{
		    Log("Disposing of Database instance");
            _db.StopBackgroundWokers();
			_db.Dispose();
			base.Dispose();
		}      			

        private static void Log(string text, params object[] args)
        {
            Trace.WriteLine(String.Format(text, args));
            Console.WriteLine(text, args);
        }

        private void SanityCheckData()
        {
            var groupedEvents = _data.GroupBy(x => x.Manufacturer)
                                        .Select(x => new { x.Key, Count = x.Count() })
                                        .ToList();           

            QueryResult queryResult;
            foreach (var item in groupedEvents)
            {
                queryResult = WaitForQueryToComplete("cameraInfo", new IndexQuery
                {
                    Query = "Manufacturer:" + item.Key
                });
                Assert.Equal(item.Count, queryResult.TotalResults);
            }

            queryResult = WaitForQueryToComplete("cameraInfo", new IndexQuery
                                {
                                    //[ ] in Lucene is INCLUSIVE (like >= and <=) 
                                    //{ } is EXCLUSIVE (live < and >)
                                    Query = "Cost_Range:[Dx100.0 TO Dx300.0]"
                                });
            Assert.Equal(_data.Where(x => x.Cost >= 100m && x.Cost <= 300m).Count(),
                        queryResult.TotalResults);

            queryResult = WaitForQueryToComplete("cameraInfo", new IndexQuery
                                {
                                    Query = "Megapixels_Range:{Dx5.4 TO Dx7.8}"
                                });
            Assert.Equal(_data.Where(x => x.Megapixels > 5.4m && x.Megapixels < 7.8m).Count(),
                        queryResult.TotalResults);

            queryResult = WaitForQueryToComplete("cameraInfo", new IndexQuery
                                {
                                    Query = "Megapixels_Range:[Dx1.0 TO Dx1.4]",
                                    SortedFields = new[] { new SortedField("Id_Range") }
                                });
            var sortedData = _data.Where(x => x.Megapixels > 1.0m && x.Megapixels < 1.4m)
                                    .OrderBy(x => x.Id)
                                    .ToList();
            var sortedResults = queryResult.Results
                                    .Select(x => x.JsonDeserialization<Camera>())
                                    .ToList();
            var counter = 0;
            foreach (var camera in sortedResults)
            {
                Assert.Equal(camera, sortedData[counter]);
                counter++;
            }
        }

        private QueryResult WaitForQueryToComplete(string indexName, IndexQuery query)
        {
            QueryResult queryResult;
            int counter = 0;
            do
            {
                queryResult = _db.Query(indexName, query);
                Log(" {0}) Query \"{1}\", Index \"{2}\", {3} results, {4}", 
                        counter, query.Query, indexName, queryResult.TotalResults, 
                        queryResult.IsStale ? "STALE" : "not stale");
                if (queryResult.IsStale)
                    Thread.Sleep(5000);
                counter++;
            } while (queryResult.IsStale);

            return queryResult;
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
