using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System.Processors;

public abstract class AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues : IDisposable
{
    private readonly RequestHandler _requestHandler;
    private readonly string _databaseName;

    protected AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues([NotNull] RequestHandler requestHandler, [NotNull] string databaseName)
    {
        _requestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
        _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
    }

    protected abstract void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long durationInMs, long totalDocumentsSizeInBytes);

    public async ValueTask ExecuteAsync()
    {
        var keys = _requestHandler.GetStringValuesQueryString("key", required: false);

        using (_requestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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

        var start = _requestHandler.GetStart();
        var pageSize = _requestHandler.GetPageSize();

        var startsWithKey = _requestHandler.GetStringQueryString("startsWith", false);
        var items = _requestHandler.ServerStore.Cluster.GetCompareExchangeValuesStartsWith(context, _databaseName, CompareExchangeKey.GetStorageKey(_databaseName, startsWithKey), start, pageSize);

        var numberOfResults = 0;
        long totalDocumentsSizeInBytes = 0;
        await using (var writer = new AsyncBlittableJsonTextWriter(context, _requestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WriteArray(context, "Results", items,
                (textWriter, operationContext, item) =>
                {
                    numberOfResults++;
                    totalDocumentsSizeInBytes += item.Value?.Size ?? 0;
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
            _requestHandler.HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
    }

    private async Task GetCompareExchangeValuesByKey(TransactionOperationContext context, Microsoft.Extensions.Primitives.StringValues keys)
    {
        var sw = Stopwatch.StartNew();

        var items = new List<(string Key, long Index, BlittableJsonReaderObject Value)>(keys.Count);
        foreach (var key in keys)
        {
            var item = _requestHandler.ServerStore.Cluster.GetCompareExchangeValue(context, CompareExchangeKey.GetStorageKey(_databaseName, key));
            if (item.Value == null && keys.Count == 1)
            {
                _requestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            items.Add((key, item.Index, item.Value));
        }

        var numberOfResults = 0;
        long totalDocumentsSizeInBytes = 0;
        await using (var writer = new AsyncBlittableJsonTextWriter(context, _requestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WriteArray(context, "Results", items,
                (textWriter, operationContext, item) =>
                {
                    numberOfResults++;
                    totalDocumentsSizeInBytes += item.Value?.Size ?? 0;

                    operationContext.Write(textWriter, new DynamicJsonValue
                    {
                        [nameof(CompareExchangeListItem.Key)] = item.Key,
                        [nameof(CompareExchangeListItem.Value)] = item.Value,
                        [nameof(CompareExchangeListItem.Index)] = item.Index
                    });
                });

            writer.WriteEndObject();
        }

        AddPagingPerformanceHint(PagingOperationType.CompareExchange, nameof(GetCompareExchangeValuesByKey), _requestHandler.HttpContext.Request.QueryString.Value,
            numberOfResults, keys.Count, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
    }

    public void Dispose()
    {
    }

    internal class CompareExchangeListItem
    {
        public string Key { get; set; }
        public object Value { get; set; }
        public long Index { get; set; }
    }
}
