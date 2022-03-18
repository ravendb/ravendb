using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System.Processors.CompareExchange;

internal abstract class AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues<TRequestHandler> : AbstractHandlerProcessor<TRequestHandler, TransactionOperationContext>
    where TRequestHandler : RequestHandler
{
    [NotNull]
    private readonly string _databaseName;

    protected AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues([NotNull] TRequestHandler requestHandler, [NotNull] string databaseName)
        : base(requestHandler, requestHandler.ServerStore.ContextPool)
    {
        _databaseName = databaseName;
    }

    protected abstract void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long durationInMs, long totalDocumentsSizeInBytes);

    public override async ValueTask ExecuteAsync()
    {
        var keys = RequestHandler.GetStringValuesQueryString("key", required: false);

        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            if (keys.Count > 0)
                await GetCompareExchangeValuesByKey(context, keys);
            else
                await GetCompareExchangeValues(context);
        }
    }


    private async Task GetCompareExchangeValues(TransactionOperationContext context)
    {
        var sw = Stopwatch.StartNew();

        var start = RequestHandler.GetStart();
        var pageSize = RequestHandler.GetPageSize();

        var startsWithKey = RequestHandler.GetStringQueryString("startsWith", false);
        var items = RequestHandler.ServerStore.Cluster.GetCompareExchangeValuesStartsWith(context, _databaseName, CompareExchangeKey.GetStorageKey(_databaseName, startsWithKey), start, pageSize);

        var numberOfResults = 0;
        long totalDocumentsSizeInBytes = 0;
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WriteArray(context, "Results", items,
                (textWriter, operationContext, item) =>
                {
                    numberOfResults++;
                    totalDocumentsSizeInBytes += item.Value?.Size ?? 0;
                    operationContext.Write(textWriter, new DynamicJsonValue
                    {
                        [nameof(CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem.Key)] = item.Key.Key,
                        [nameof(CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem.Value)] = item.Value,
                        [nameof(CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem.Index)] = item.Index
                    });
                });

            writer.WriteEndObject();
        }

        AddPagingPerformanceHint(PagingOperationType.CompareExchange, nameof(ClusterStateMachine.GetCompareExchangeValuesStartsWith),
            HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
    }

    private async Task GetCompareExchangeValuesByKey(TransactionOperationContext context, Microsoft.Extensions.Primitives.StringValues keys)
    {
        var sw = Stopwatch.StartNew();

        var items = new List<(string Key, long Index, BlittableJsonReaderObject Value)>(keys.Count);
        foreach (var key in keys)
        {
            var item = RequestHandler.ServerStore.Cluster.GetCompareExchangeValue(context, CompareExchangeKey.GetStorageKey(_databaseName, key));
            if (item.Value == null && keys.Count == 1)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            items.Add((key, item.Index, item.Value));
        }

        var numberOfResults = 0;
        long totalDocumentsSizeInBytes = 0;
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WriteArray(context, "Results", items,
                (textWriter, operationContext, item) =>
                {
                    numberOfResults++;
                    totalDocumentsSizeInBytes += item.Value?.Size ?? 0;

                    operationContext.Write(textWriter, new DynamicJsonValue
                    {
                        [nameof(CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem.Key)] = item.Key,
                        [nameof(CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem.Value)] = item.Value,
                        [nameof(CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem.Index)] = item.Index
                    });
                });

            writer.WriteEndObject();
        }

        AddPagingPerformanceHint(PagingOperationType.CompareExchange, nameof(GetCompareExchangeValuesByKey), HttpContext.Request.QueryString.Value,
            numberOfResults, keys.Count, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
    }
}
