using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    class CompareExchangeHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/cmpxchg", "GET", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public Task GetCompareExchangeValues()
        {
            var keys = GetStringValuesQueryString("key", required: false);
            
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (keys.Count > 0)
                    GetCompareExchangeValuesByKey(context, keys);
                else
                    GetCompareExchangeValues(context);
            }
            
            return Task.CompletedTask;
        }

        private void GetCompareExchangeValues(TransactionOperationContext context)
        {
            var sw = Stopwatch.StartNew();

            var start = GetStart();
            var pageSize = GetPageSize();

            var startsWithKey = GetStringQueryString("startsWith", false);
            var items = ServerStore.Cluster.GetCompareExchangeValuesStartsWith(context, Database.Name, CompareExchangeKey.GetStorageKey(Database.Name, startsWithKey), start, pageSize);

            var numberOfResults = 0;
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                
                writer.WriteArray(context, "Results", items,
                    (textWriter, operationContext, item) =>
                    {
                        numberOfResults++;
                        operationContext.Write(textWriter, new DynamicJsonValue
                        {
                            [nameof(CompareExchangeListItem.Key)] = item.Key.Key,
                            [nameof(CompareExchangeListItem.Value)] = item.Value,
                            [nameof(CompareExchangeListItem.Index)] = item.Index
                        });
                    });

                writer.WriteEndObject();
            }

            AddPagingPerformanceHint(PagingOperationType.CompareExchange, nameof(ClusterStateMachine.GetCompareExchangeValuesStartsWith), 
                HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds);
        }
        
        public class CompareExchangeListItem
        {
            public string Key { get; set; }
            public object Value { get; set; }
            public long Index { get; set; }
        }
        
        private void GetCompareExchangeValuesByKey(TransactionOperationContext context, Microsoft.Extensions.Primitives.StringValues keys)
        {
            var sw = Stopwatch.StartNew();

            var items = new List<(string Key, long Index, BlittableJsonReaderObject Value)>(keys.Count);
            foreach (var key in keys)
            {
                var item = ServerStore.Cluster.GetCompareExchangeValue(context, CompareExchangeKey.GetStorageKey(Database.Name, key));
                if (item.Value == null && keys.Count == 1)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                items.Add((key, item.Index, item.Value));
            }

            var numberOfResults = 0;
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                
                writer.WriteArray(context, "Results", items,
                    (textWriter, operationContext, item) =>
                    {
                        numberOfResults++;
                        operationContext.Write(textWriter, new DynamicJsonValue
                        {
                            ["Key"] = item.Key,
                            ["Value"] = item.Value,
                            ["Index"] = item.Index
                        });
                    });

                writer.WriteEndObject();
            }

            AddPagingPerformanceHint(PagingOperationType.CompareExchange, nameof(GetCompareExchangeValuesByKey), HttpContext.Request.QueryString.Value, 
                numberOfResults, keys.Count, sw.ElapsedMilliseconds);
        }


        [RavenAction("/databases/*/cmpxchg", "PUT", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task PutCompareExchangeValue()
        {
            var key = GetStringQueryString("key");
            var raftRequestId = GetRaftRequestIdFromQuery();

            // ReSharper disable once PossibleInvalidOperationException
            var index = GetLongQueryString("index", true).Value;

            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var updateJson = await context.ReadForMemoryAsync(RequestBodyStream(), "read-unique-value");
                var command = new AddOrUpdateCompareExchangeCommand(Database.Name, key, updateJson, index, context, raftRequestId);
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    (var raftIndex, var response) = await ServerStore.SendToLeaderAsync(context, command);
                    await ServerStore.Cluster.WaitForIndexNotification(raftIndex);

                    var result = (CompareExchangeCommandBase.CompareExchangeResult)response;
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(CompareExchangeResult<object>.Index)] = result.Index,
                        [nameof(CompareExchangeResult<object>.Value)] = result.Value,
                        [nameof(CompareExchangeResult<object>.Successful)] = result.Index == raftIndex
                    });
                }
            }
        }
        
        [RavenAction("/databases/*/cmpxchg", "DELETE", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task DeleteCompareExchangeValue()
        {
            var key = GetStringQueryString("key");
            var raftRequestId = GetRaftRequestIdFromQuery();

            // ReSharper disable once PossibleInvalidOperationException
            var index = GetLongQueryString("index", true).Value;

            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var command = new RemoveCompareExchangeCommand(Database.Name, key, index, context, raftRequestId);
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    (var raftIndex, var response) = await ServerStore.SendToLeaderAsync(context, command);
                    await ServerStore.Cluster.WaitForIndexNotification(raftIndex);

                    var result = (CompareExchangeCommandBase.CompareExchangeResult)response;
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(CompareExchangeResult<object>.Index)] = result.Index,
                        [nameof(CompareExchangeResult<object>.Value)] = result.Value,
                        [nameof(CompareExchangeResult<object>.Successful)] = result.Index == raftIndex
                    });
                }
            }
        }
    }
}
