using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    internal class CompareExchangeHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/cmpxchg", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetCompareExchangeValues()
        {
            using (var processor = new CompareExchangeHandlerProcessorForGetCompareExchangeValues(this, Database.Name))
                await processor.ExecuteAsync();
        }


        [RavenAction("/databases/*/cmpxchg", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
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
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    ServerStore.ForTestingPurposes?.ModifyCompareExchangeTimeout?.Invoke(command);
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

        [RavenAction("/databases/*/cmpxchg", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
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
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
