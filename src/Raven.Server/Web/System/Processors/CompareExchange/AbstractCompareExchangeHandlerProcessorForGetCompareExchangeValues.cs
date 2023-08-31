using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System.Processors.CompareExchange;

internal abstract class AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    private readonly AbstractCompareExchangeStorage _compareExchangeStorage;

    protected AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues([NotNull] TRequestHandler requestHandler, [NotNull] AbstractCompareExchangeStorage compareExchangeStorage) : base(requestHandler)
    {
        _compareExchangeStorage = compareExchangeStorage ?? throw new ArgumentNullException(nameof(compareExchangeStorage));
    }

    public override async ValueTask ExecuteAsync()
    {
        var keys = RequestHandler.GetStringValuesQueryString("key", required: false);

        using (ClusterContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            if (keys.Count > 0)
                await GetCompareExchangeValuesByKey(context, keys);
            else
                await GetCompareExchangeValues(context);
        }
    }

    private async Task GetCompareExchangeValues(ClusterOperationContext context)
    {
        var sw = Stopwatch.StartNew();

        var start = RequestHandler.GetStart();
        var pageSize = RequestHandler.GetPageSize();

        var startsWithKey = RequestHandler.GetStringQueryString("startsWith", false);
        var items = _compareExchangeStorage.GetCompareExchangeValuesStartsWith(context, startsWithKey, start, pageSize);

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

        RequestHandler.AddPagingPerformanceHint(PagingOperationType.CompareExchange, nameof(AbstractCompareExchangeStorage.GetCompareExchangeValuesStartsWith),
            HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
    }

    private async Task GetCompareExchangeValuesByKey(ClusterOperationContext context, Microsoft.Extensions.Primitives.StringValues keys)
    {
        var sw = Stopwatch.StartNew();

        var items = new List<(string Key, long Index, BlittableJsonReaderObject Value)>(keys.Count);
        foreach (var key in keys)
        {
            var item = _compareExchangeStorage.GetCompareExchangeValue(context, key);
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

        RequestHandler.AddPagingPerformanceHint(PagingOperationType.CompareExchange, nameof(GetCompareExchangeValuesByKey), HttpContext.Request.QueryString.Value,
            numberOfResults, keys.Count, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
    }
}
