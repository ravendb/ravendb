using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Debugging.Processors;
using Raven.Server.Documents.Queries;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class QueriesDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/queries/kill", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task KillQuery()
        {
            using (var processor = new QueriesDebugHandlerProcessorForKillQuery(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/debug/queries/running", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task RunningQueries()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                var isFirst = true;
                foreach (var group in Database.QueryRunner.CurrentlyRunningQueries.GroupBy(x => x.IndexName))
                {
                    if (isFirst == false)
                        writer.WriteComma();
                    isFirst = false;

                    writer.WritePropertyName(group.Key);
                    writer.WriteStartArray();

                    var isFirstInternal = true;
                    foreach (var query in group)
                    {
                        if (isFirstInternal == false)
                            writer.WriteComma();

                        isFirstInternal = false;

                        query.Write(writer, context);
                    }

                    writer.WriteEndArray();
                }

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/debug/queries/cache/list", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task QueriesCacheList()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                var queryCache = Database.QueryMetadataCache.GetQueryCache();

                var queriesList = new List<DynamicJsonValue>();

                writer.WriteStartObject();

                writer.WritePropertyName("TotalCachedQueries");
                writer.WriteInteger(queryCache.Length);
                writer.WriteComma();

                foreach (var item in queryCache)
                {
                    if (item != null)
                    {
                        var curDjvItem = new DynamicJsonValue();
                        queriesList.Add(curDjvItem);

                        curDjvItem[nameof(QueryMetadata.CreatedAt)] = item.CreatedAt;

                        curDjvItem[nameof(QueryMetadata.LastQueriedAt)] = item.LastQueriedAt;
                        if (item.IsGroupBy)
                        {
                            curDjvItem[nameof(QueryMetadata.IsGroupBy)] = true;
                        }

                        if (item.IsDistinct)
                        {
                            curDjvItem[nameof(QueryMetadata.IsDistinct)] = true;
                        }

                        if (item.HasFacet)
                        {
                            curDjvItem[nameof(QueryMetadata.HasFacet)] = true;
                        }

                        if (item.HasMoreLikeThis)
                        {
                            curDjvItem[nameof(QueryMetadata.HasMoreLikeThis)] = true;
                        }

                        if (item.HasSuggest)
                        {
                            curDjvItem[nameof(QueryMetadata.HasSuggest)] = true;
                        }

                        if (item.OrderBy != null)
                        {
                            curDjvItem["Sorted"] = true;
                        }

                        if (item.HasCmpXchg)
                        {
                            curDjvItem[nameof(QueryMetadata.HasCmpXchg)] = true;
                        }

                        if (item.HasExplanations)
                        {
                            curDjvItem[nameof(QueryMetadata.HasExplanations)] = true;
                        }

                        if (item.HasIntersect)
                        {
                            curDjvItem[nameof(QueryMetadata.HasIntersect)] = true;
                        }

                        if (item.IsDynamic)
                        {
                            curDjvItem[nameof(QueryMetadata.IsDynamic)] = true;
                        }

                        if (item.SelectFields != null && item.SelectFields.Any(x => x.Function != null))
                        {
                            curDjvItem["IsJSProjection"] = true;
                        }

                        if (string.IsNullOrEmpty(item.CollectionName) == false)
                        {
                            curDjvItem[nameof(QueryMetadata.CollectionName)] = item.CollectionName;
                        }

                        if (string.IsNullOrEmpty(item.AutoIndexName) == false)
                        {
                            curDjvItem[nameof(QueryMetadata.AutoIndexName)] = item.AutoIndexName;
                        }

                        if (string.IsNullOrEmpty(item.IndexName) == false)
                        {
                            curDjvItem[nameof(QueryMetadata.IndexName)] = item.IndexName;
                        }

                        curDjvItem[nameof(QueryMetadata.QueryText)] = item.QueryText;
                    }
                }

                writer.WriteArray("Results", queriesList, context);
                writer.WriteEndObject();
            }
        }
    }
}
