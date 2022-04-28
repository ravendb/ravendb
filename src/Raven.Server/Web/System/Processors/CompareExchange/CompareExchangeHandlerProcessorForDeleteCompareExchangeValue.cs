using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System.Processors.CompareExchange;

internal class CompareExchangeHandlerProcessorForDeleteCompareExchangeValue<TOperationContext> : AbstractDatabaseHandlerProcessor<TOperationContext> 
    where TOperationContext : JsonOperationContext
{
    public CompareExchangeHandlerProcessorForDeleteCompareExchangeValue([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var key = RequestHandler.GetStringQueryString("key");
        var raftRequestId = RequestHandler.GetRaftRequestIdFromQuery();

        // ReSharper disable once PossibleInvalidOperationException
        var index = RequestHandler.GetLongQueryString("index", true).Value;

        await RequestHandler.ServerStore.EnsureNotPassiveAsync();

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var command = new RemoveCompareExchangeCommand(RequestHandler.DatabaseName, key, index, context, raftRequestId);
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                (var raftIndex, var response) = await RequestHandler.ServerStore.SendToLeaderAsync(context, command);
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
