using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class QueriesDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/queries/kill", "POST", AuthorizationStatus.ValidUser)]
        public Task KillQuery()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("indexName");
            var id = GetLongQueryString("id");

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            var query = index.CurrentlyRunningQueries
                .FirstOrDefault(q => q.QueryId == id);

            if (query == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            query.Token.Cancel();

            return NoContent();
        }

        [RavenAction("/databases/*/debug/queries/running", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public Task RunningQueries()
        {
            var indexes = Database
                .IndexStore
                .GetIndexes()
                .ToList();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                var isFirst = true;
                foreach (var index in indexes)
                {
                    if (isFirst == false)
                        writer.WriteComma();
                    isFirst = false;

                    writer.WritePropertyName(index.Name);
                    writer.WriteStartArray();

                    var isFirstInternal = true;
                    foreach (var query in index.CurrentlyRunningQueries)
                    {
                        if (isFirstInternal == false)
                            writer.WriteComma();

                        isFirstInternal = false;

                        writer.WriteStartObject();

                        writer.WritePropertyName((nameof(query.Duration)));
                        writer.WriteString(query.Duration.ToString());
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(query.QueryId)));
                        writer.WriteInteger(query.QueryId);
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(query.StartTime)));
                        writer.WriteDateTime(query.StartTime, isUtc: true);
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(query.QueryInfo)));
                        writer.WriteIndexQuery(context, query.QueryInfo);

                        writer.WriteEndObject();
                    }

                    writer.WriteEndArray();
                }

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }
       
        [RavenAction("/databases/*/debug/queries/cache/list", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public Task QueriesCacheList()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var queryCache = Database.QueryMetadataCache.GetQueryCache();

                var responseDjv = new DynamicJsonValue();
                var queriesList = new DynamicJsonArray();

                responseDjv["CachedQueryMetadataItems"] = queriesList;

                foreach (var item in queryCache)
                {   
                    if (item != null)
                    {
                        var curDjvItem = new DynamicJsonValue();
                        queriesList.Add(curDjvItem);

                        if (item.IsGroupBy)
                        {
                            curDjvItem["IsGroupBy"] = true;                            
                        }

                        if (item.IsDistinct)
                        {
                            curDjvItem["IsDistinct"] = true;                                                        
                        }

                        if (item.HasFacet)
                        {
                            curDjvItem["HasFacet"] = true;                            
                        }

                        if (item.HasMoreLikeThis)
                        {
                            curDjvItem["HasMoreLikeThis"] = true;                            
                        }

                        if (item.HasSuggest)
                        {
                            curDjvItem["HasSuggest"] = true;                            
                        }

                        if (item.OrderBy != null)
                        {
                            curDjvItem["OrderBy"] = true;                            
                        }

                        if (item.HasCmpXchg)
                        {
                            curDjvItem["HasCmpXchg"] = true;
                        }

                        if (item.HasExplanations)
                        {
                            curDjvItem["HasExplanations"] = true;
                        }

                        if (item.HasIntersect)
                        {
                            curDjvItem["HasIntersect"] = true;
                        }

                        if (item.IsDynamic)
                        {
                            curDjvItem["IsDynamic"] = true;
                        }

                        if (item.IsOptimizedSortOnly)
                        {
                            curDjvItem["IsOptimizedSortOnly"] = true;
                        }        

                        if (item.SelectFields.Any(x => x.Function != null))
                        {
                            curDjvItem["IsJSProjection"] = true;
                        }
                        
                        if (string.IsNullOrEmpty(item.CollectionName) == false)
                            curDjvItem["CollectionName"] = item.CollectionName;                                                

                        if (string.IsNullOrEmpty(item.AutoIndexName) == false)
                        {
                            curDjvItem["AutoIndexName"] = item.AutoIndexName;                                
                        }

                        if (string.IsNullOrEmpty(item.IndexName) == false)
                        {
                            curDjvItem["IndexName"] = item.IndexName;
                        }                        
                    
                        curDjvItem["QueryText"] = item.QueryText;                        
                    }
                }

                var blittableResult = context.ReadObject(responseDjv, "debug/queries/cache/list");

                writer.WriteObject(blittableResult);


            }

            return Task.CompletedTask;
        }
    }
}
