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
            // Create the map
            var map = DynamicQueryMapping.Create(query.Query);

            // Get the index name
            string indexName = CreateOrGetDynamicIndexName(database, map);

            // Re-write the query
            string realQuery = query.Query;
            foreach (var mapItem in map.Items)
            {
                realQuery = realQuery.Replace(mapItem.From, mapItem.To);
            }

            // Perform the query until we have some results at least
            QueryResult result = null;
            var sp = Stopwatch.StartNew();
            while (true)
            {
                result = database.Query(indexName,
                   new Raven.Database.Data.IndexQuery()
                   {
                       Cutoff = query.CutOff,
                       PageSize = query.PageSize,
                       Query = realQuery,
                       Start = query.Start
                   });

                if (result.IsStale && result.Results.Count < query.PageSize)
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
            
            return new QueryResults()
            {
                Results = result.Results.ToArray()
            };
        }

        private static string CreateOrGetDynamicIndexName(DocumentDatabase database, DynamicQueryMapping map)
        {
            // This isn't sustainable with long dynamic queries
            String combinedFields = String.Join("", 
                map.Items
                .OrderBy(x => x.To)
                .Select(x=>x.To)
                .ToArray());

            // Need to use an appropriate index name based on the fields passed in
            var indexName = String.Format("Temp_{0}", combinedFields);

            var mapping = map.Items
              .Select(x => string.Format("{0} = doc.{1}", x.To, x.From))
              .ToArray();

            // Create the definition
            var definition = new IndexDefinition()
            {
                Map = @"from doc in docs select new 
                 { 
                    " + String.Join(",\r\n", mapping) + @"
                 }",
            };

            // Store the index - this will check for the present of the index for us
            database.PutIndex(indexName, definition);

            // And return the index name
            return indexName;
        }
    }
}
