using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations;
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
        public Task GetCmpXchgValue()
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
                        [nameof(RawClusterValueResult.Index)] = res.Index,
                        [nameof(RawClusterValueResult.Value)] = res.Value,
                        [nameof(RawClusterValueResult.Successful)] = true
                    });
                    writer.Flush();
                }
                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/cmpxchg", "PUT", AuthorizationStatus.ValidUser)]
        public async Task PutCmpXchgValue()
        {
            var prefix = Database.Name + "/";
            var key = prefix + GetStringQueryString("key");
            
            // ReSharper disable once PossibleInvalidOperationException
            var index = GetLongQueryString("index", true).Value;

            ServerStore.EnsureNotPassive();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var updateJson = await context.ReadForMemoryAsync(RequestBodyStream(), "read-unique-value");
                var command = new CompareExchangeCommand(key, updateJson, index);
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    (var raftIndex, _) = await ServerStore.SendToLeaderAsync(command);
                    await ServerStore.Cluster.WaitForIndexNotification(raftIndex);
                    using (context.OpenReadTransaction())
                    {
                        var res = ServerStore.Cluster.GetCmpXchg(context, key);
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(RawClusterValueResult.Index)] = res.Index,
                            [nameof(RawClusterValueResult.Value)] = res.Value,
                            [nameof(RawClusterValueResult.Successful)] = res.Index == raftIndex
                        });
                    }
                    writer.Flush();
                }
            }
        }
    }
}
