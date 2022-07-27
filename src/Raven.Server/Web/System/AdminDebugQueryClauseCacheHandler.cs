using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Raven.Server.Documents.Queries.LuceneIntegration;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;

namespace Raven.Server.Web.System
{
    public class AdminDebugQueryClauseCacheHandler : RequestHandler
    {
        [RavenAction("/admin/indexes/lucene/query-clause-cache", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task QueryClauseCache()
        {
            var entries = new DynamicJsonArray();

            long totalSize = 0;
            long totalItems = 0;

            foreach (var database in GetEntriesForDebug().GroupBy(x => x.Query.Database))
            {
                var indexesJson = new DynamicJsonArray();
                entries.Add(new DynamicJsonValue { ["Database"] = database.Key, ["Indexes"] = indexesJson });
                foreach (var index in database.GroupBy(x => x.Query.Index))
                {
                    long totalIndexItems = 0, totalIndexSize = 0;
                    var queriesJson = new DynamicJsonArray();

                    foreach (var query in index.GroupBy(x => x.Query.Query))
                    {
                        long querySize = 0, queryItems = 0;
                        foreach (var (_, fba) in query)
                        {
                            querySize += fba.Size.GetValue(SizeUnit.Bytes);
                            queryItems++;
                        }

                        totalIndexItems += queryItems;
                        totalIndexSize += querySize;

                        queriesJson.Add(new DynamicJsonValue
                        {
                            ["Query"] = query.Key, 
                            ["Size"] = querySize, 
                            ["HumaneSize"] = new Size(querySize, SizeUnit.Bytes).ToString(), 
                            ["Total"] = queryItems
                        });
                    }


                    indexesJson.Add(new DynamicJsonValue
                    {
                        ["Index"] = index.Key,
                        ["Queries"] = queriesJson,
                        ["Size"] = totalIndexSize,
                        ["HumaneSize"] = new Size(totalIndexSize, SizeUnit.Bytes).ToString(),
                        ["Total"] = totalIndexItems,
                    });

                    totalItems += totalIndexItems;
                    totalSize += totalIndexSize;
                }
            }

            var result = new DynamicJsonValue { ["Summary"] = new DynamicJsonValue
            {
                ["Size"] = totalSize,
                ["HumaneSize"] = new Size(totalSize, SizeUnit.Bytes).ToString(),
                ["Total"] = totalItems,
            }, ["Entries"] = entries, };
            
            using var _ = ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context);
            await using (var write = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(write, result);
            }
            
        }

        private IEnumerable<(CachingQuery.QueryCacheKey Query, FastBitArray Cached)> GetEntriesForDebug()
        {
            foreach (var (key, value) in Server.ServerStore.QueryClauseCache.EntriesForDebug)
            {
                if (key is not CachingQuery.QueryCacheKey query || value is not FastBitArray fba)
                    continue;

                yield return (query, fba);
            }
        }
    }
}
