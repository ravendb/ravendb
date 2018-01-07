using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    class CompareExchangeHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/cmpxchg", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCompareExchangeValue()
        {
            var prefix = Database.Name + "/";
            var key = prefix + GetStringQueryString("key");
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var res = ServerStore.Cluster.GetCmpXchg(context, key);
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(CompareExchangeResult<object>.Index)] = res.Index,
                        [nameof(CompareExchangeResult<object>.Value)] = res.Value,
                        [nameof(CompareExchangeResult<object>.Successful)] = true
                    });
                    writer.Flush();
                }
            }
            
            return Task.CompletedTask;
        }
        
        [RavenAction("/databases/*/cmpxchg/list", "GET", AuthorizationStatus.ValidUser)]
        public Task ListCompareExchangeValues()
        {
            var prefix = Database.Name + "/";
            var key = prefix + GetStringQueryString("startsWith", false);
            var page = GetStart();
            var size = GetPageSize();
            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    using (context.OpenReadTransaction())
                        writer.WriteArray(context, "Results", ServerStore.Cluster.GetCmpXchgByPrefix(context, Database.Name, key, page, size), 
                            (textWriter, operationContext, item) =>
                            {
                                operationContext.Write(textWriter,new DynamicJsonValue
                                {
                                    ["Key"] = item.Key,
                                    ["Value"] = item.Value,
                                    ["Index"] = item.Index
                                });
                            });
                    writer.WriteEndObject();
                    writer.Flush();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/cmpxchg", "PUT", AuthorizationStatus.ValidUser)]
        public async Task PutCompareExchangeValue()
        {
            var prefix = Database.Name + "/";
            var key = prefix + GetStringQueryString("key");

            // ReSharper disable once PossibleInvalidOperationException
            var index = GetLongQueryString("index", true).Value;

            ServerStore.EnsureNotPassive();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var updateJson = await context.ReadForMemoryAsync(RequestBodyStream(), "read-unique-value");
                var command = new AddOrUpdateCompareExchangeCommand(key, updateJson, index);
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    (var raftIndex, var res) = await ServerStore.SendToLeaderAsync(command);
                    await ServerStore.Cluster.WaitForIndexNotification(raftIndex);
                    using (context.OpenReadTransaction())
                    {
                        var tuple = ((long Index, object Value))res;
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(CompareExchangeResult<object>.Index)] = tuple.Index,
                            [nameof(CompareExchangeResult<object>.Value)] = tuple.Value,
                            [nameof(CompareExchangeResult<object>.Successful)] = tuple.Index == raftIndex
                        });
                    }
                    writer.Flush();
                }
            }
        }
        
        [RavenAction("/databases/*/cmpxchg", "DELETE", AuthorizationStatus.ValidUser)]
        public async Task DeleteCompareExchangeValue()
        {
            var prefix = Database.Name + "/";
            var key = prefix + GetStringQueryString("key");

            // ReSharper disable once PossibleInvalidOperationException
            var index = GetLongQueryString("index", true).Value;

            ServerStore.EnsureNotPassive();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var command = new RemoveCompareExchangeCommand(key, index);
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    (var raftIndex, var res) = await ServerStore.SendToLeaderAsync(command);
                    await ServerStore.Cluster.WaitForIndexNotification(raftIndex);
                    using (context.OpenReadTransaction())
                    {
                        var tuple = ((long Index, object Value))res;
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(CompareExchangeResult<object>.Index)] = tuple.Index,
                            [nameof(CompareExchangeResult<object>.Value)] = tuple.Value,
                            [nameof(CompareExchangeResult<object>.Successful)] = tuple.Index == raftIndex
                        });
                    }
                    writer.Flush();
                }
            }
        }
    }
}
