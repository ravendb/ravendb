using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System.Processors.CompareExchange;

public sealed class CompareExchangeHandlerProcessorForPutCompareExchangeValue : IDisposable
{
    private readonly RequestHandler _requestHandler;
    private readonly string _databaseName;

    public CompareExchangeHandlerProcessorForPutCompareExchangeValue([NotNull] RequestHandler requestHandler, [NotNull] string databaseName)
    {
        _requestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
        _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
    }

    public async ValueTask ExecuteAsync()
    {
        var key = _requestHandler.GetStringQueryString("key");
        var raftRequestId = _requestHandler.GetRaftRequestIdFromQuery();

        // ReSharper disable once PossibleInvalidOperationException
        var index = _requestHandler.GetLongQueryString("index", true).Value;

        await _requestHandler.ServerStore.EnsureNotPassiveAsync();

        using (_requestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var updateJson = await context.ReadForMemoryAsync(_requestHandler.RequestBodyStream(), "read-unique-value");
            var command = new AddOrUpdateCompareExchangeCommand(_databaseName, key, updateJson, index, context, raftRequestId);
            await using (var writer = new AsyncBlittableJsonTextWriter(context, _requestHandler.ResponseBodyStream()))
            {
                _requestHandler.ServerStore.ForTestingPurposes?.ModifyCompareExchangeTimeout?.Invoke(command);
                (var raftIndex, var response) = await _requestHandler.ServerStore.SendToLeaderAsync(context, command);
                await _requestHandler.ServerStore.Cluster.WaitForIndexNotification(raftIndex);

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

    public void Dispose()
    {
    }
}
