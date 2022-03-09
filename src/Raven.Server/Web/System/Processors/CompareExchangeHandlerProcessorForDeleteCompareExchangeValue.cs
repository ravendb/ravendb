using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System.Processors;

internal class CompareExchangeHandlerProcessorForDeleteCompareExchangeValue : AbstractHandlerProcessor<RequestHandler, TransactionOperationContext>
{
    private readonly string _databaseName;

    public CompareExchangeHandlerProcessorForDeleteCompareExchangeValue([NotNull] RequestHandler requestHandler, [NotNull] string databaseName)
        : base(requestHandler, requestHandler.ServerStore.ContextPool)
    {
        _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
    }

    public override async ValueTask ExecuteAsync()
    {
        var key = RequestHandler.GetStringQueryString("key");
        var raftRequestId = RequestHandler.GetRaftRequestIdFromQuery();

        // ReSharper disable once PossibleInvalidOperationException
        var index = RequestHandler.GetLongQueryString("index", true).Value;

        await RequestHandler.ServerStore.EnsureNotPassiveAsync();

        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var command = new RemoveCompareExchangeCommand(_databaseName, key, index, context, raftRequestId);
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                (var raftIndex, var response) = await RequestHandler.ServerStore.SendToLeaderAsync(context, command);
                await RequestHandler.ServerStore.Cluster.WaitForIndexNotification(raftIndex);

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
