using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
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
            using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), Database.DatabaseShutdown))
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

                        query.Write(writer, context);
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

                        curDjvItem[nameof(Queries.QueryMetadata.CreatedAt)] = item.CreatedAt;

                        curDjvItem[nameof(Queries.QueryMetadata.LastQueriedAt)] = item.LastQueriedAt;
                        if (item.IsGroupBy)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.IsGroupBy)] = true;
                        }

                        if (item.IsDistinct)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.IsDistinct)] = true;
                        }

                        if (item.HasFacet)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.HasFacet)] = true;
                        }

                        if (item.HasMoreLikeThis)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.HasMoreLikeThis)] = true;
                        }

                        if (item.HasSuggest)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.HasSuggest)] = true;
                        }

                        if (item.OrderBy != null)
                        {
                            curDjvItem["Sorted"] = true;
                        }

                        if (item.HasCmpXchg)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.HasCmpXchg)] = true;
                        }

                        if (item.HasExplanations)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.HasExplanations)] = true;
                        }

                        if (item.HasIntersect)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.HasIntersect)] = true;
                        }

                        if (item.IsDynamic)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.IsDynamic)] = true;
                        }

                        if (item.SelectFields != null && item.SelectFields.Any(x => x.Function != null))
                        {
                            curDjvItem["IsJSProjection"] = true;
                        }

                        if (string.IsNullOrEmpty(item.CollectionName) == false)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.CollectionName)] = item.CollectionName;
                        }

                        if (string.IsNullOrEmpty(item.AutoIndexName) == false)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.AutoIndexName)] = item.AutoIndexName;
                        }

                        if (string.IsNullOrEmpty(item.IndexName) == false)
                        {
                            curDjvItem[nameof(Queries.QueryMetadata.IndexName)] = item.IndexName;
                        }

                        curDjvItem[nameof(Queries.QueryMetadata.QueryText)] = item.QueryText;
                    }
                }

                writer.WriteArray("Results", queriesList, context);
                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }
    }
}
