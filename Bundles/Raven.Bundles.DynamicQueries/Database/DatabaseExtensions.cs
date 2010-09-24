using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database;
using Raven.Bundles.DynamicQueries.Data;
using Raven.Database.Data;
using System.Threading;
using System.Diagnostics;

namespace Raven.Bundles.DynamicQueries.Database
{
    public static class DatabaseExtensions
    {
        public static QueryResults ExecuteDynamicQuery(this DocumentDatabase database, DynamicQuery query)
        {
            // TODO: Check for existence of created query

            // Fall back to a temporary query
            return PerformTemporaryQuery(database, query);
        }

        private static QueryResults PerformTemporaryQuery(DocumentDatabase database, DynamicQuery query)
        {
            var indexName = String.Format("Temp_{0}", Guid.NewGuid().ToString());
            var mapping = query.FieldMap.Split(',')
                .Select(x=>{
                    var split = x.Split(':');
                    return string.Format("{0} = doc.{1}", split[1], split[0]);
                }).ToArray();

            database.PutIndex(indexName, new Raven.Database.Indexing.IndexDefinition(){
                 Map = @"from doc in docs select new 
                 { 
                    " + String.Join(",\r\n", mapping) + @"
                 }",                  
            });
            var dateStart = DateTime.Now;

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

            database.DeleteIndex(indexName);

            return new QueryResults()
            { 
                 Results = result.Results.ToArray()
            };
        }
    }
}
