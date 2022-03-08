using System;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications.Details;

namespace Raven.Server.Web.System.Processors;

public class CompareExchangeHandlerProcessorForGetCompareExchangeValues : AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues
{
    [NotNull]
    private readonly DatabaseRequestHandler _requestHandler;

    public CompareExchangeHandlerProcessorForGetCompareExchangeValues([NotNull] DatabaseRequestHandler requestHandler, [NotNull] string databaseName) : base(requestHandler, databaseName)
    {
        _requestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
    }

    protected override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long durationInMs, long totalDocumentsSizeInBytes)
    {
        _requestHandler.AddPagingPerformanceHint(PagingOperationType.CompareExchange, action, details, numberOfResults, pageSize, durationInMs, totalDocumentsSizeInBytes);
    }
}
