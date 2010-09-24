using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database;
using Raven.Bundles.DynamicQueries.Data;
using Raven.Database.Data;
using System.Threading;
using System.Diagnostics;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Database.Json;
using Raven.Database.Extensions;


namespace Raven.Bundles.DynamicQueries.Database
{
    public static class DatabaseExtensions
    {
        public static QueryResults ExecuteDynamicQuery(this DocumentDatabase database, DynamicQuery query)
        {
            // TODO: Check for existence of created query
            /* Use the below code to query that created query
            QueryResult result = null;
            var sp = Stopwatch.StartNew();
            while (true)
            {
                result = database.Query(indexName,
                   new Raven.Database.Data.IndexQuery()
                   {
                       Cutoff = dateStart,
                       PageSize = query.PageSize,
                       Query = query.Query,
                       Start = query.Start
                   });

                if (result.IsStale)
                {
                    if (sp.Elapsed.TotalMilliseconds > 10000)
                    {
                        sp.Stop();
                        break;
                    }

                    Thread.Sleep(100);
                    continue;
                }
                else
                {
                    break;
                }
            } 
             * */

            // Fall back to a temporary query
            return PerformTemporaryQuery(database, query);
        }

        private static QueryResults PerformTemporaryQuery(DocumentDatabase database, DynamicQuery query)
        {
            // This is our cut-off date, one way or another
            var dateStart = DateTime.Now;

            var indexName = String.Format("Temp_{0}", Guid.NewGuid().ToString());
            var mapping = query.Mappings
                .Select(x=> string.Format("{0} = doc.{1}", x.To, x.From))
                .ToArray();

            // Create the definition
            var definition = new IndexDefinition(){
                 Map = @"from doc in docs select new 
                 { 
                    " + String.Join(",\r\n", mapping) + @"
                 }",                  
            };
                        
            // Store the actual index definition
            database.IndexDefinitionStorage.AddIndex(indexName, definition);
            database.IndexStorage.CreateIndexImplementation(indexName, definition);
            database.TransactionalStorage.Batch(actions =>
            {
                actions.Indexing.AddIndexAsTemporary(indexName);
            });
                        
            // Perform a manual run-through of this index
            var viewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
		    var currentEtag = Guid.Empty;
            bool docsPending = true;           

            // Perform the on-demand index
            while(docsPending){
                database.TransactionalStorage.Batch(actions=>{

                    // We get some docs
                    var jsonDocs = actions.Documents.GetDocumentsAfter(currentEtag)
				    .Where(x => x != null && x.LastModified < dateStart)
				    .Take(10000)
				    .ToArray();

                    // Stop searching once we've reached cut-off
                    if(jsonDocs.Length == 0){
                        docsPending = false;
                        return;
                    }

			        var documentRetriever = new DocumentRetriever(null, database.WorkContext.ReadTriggers);
			       
                    // Perform the index
				        database.WorkContext.IndexStorage.Index(indexName, viewGenerator, 
					        jsonDocs
					        .Select(doc => documentRetriever.ProcessReadVetoes(doc, null, ReadOperation.Index))
					        .Where(doc => doc != null)
					        .Select(x => JsonToExpando.Convert(x.ToJson())), database.WorkContext, actions, dateStart);	    

                    // And update our etag
                    currentEtag = jsonDocs.Last().Etag;
                });
            }

            // Query
            var result = database.Query(indexName,
                   new IndexQuery()
                   {
                       Cutoff = dateStart,
                       PageSize = query.PageSize,
                       Query = query.Query,
                       Start = query.Start
                   });

            // Destroy the index
            database.TransactionalStorage.Batch(actions =>
            {
                actions.Indexing.DeleteIndex(indexName);
            });
            database.IndexDefinitionStorage.RemoveIndex(indexName);
            database.IndexStorage.DeleteIndex(indexName);

            return new QueryResults()
            { 
                 Results = result.Results.ToArray()
            };
        }
    }
}
