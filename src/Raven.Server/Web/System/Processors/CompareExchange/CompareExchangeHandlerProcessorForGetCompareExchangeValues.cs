using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications.Details;

namespace Raven.Server.Web.System.Processors.CompareExchange;

internal class CompareExchangeHandlerProcessorForGetCompareExchangeValues : AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues<DatabaseRequestHandler>
{
    public CompareExchangeHandlerProcessorForGetCompareExchangeValues([NotNull] DatabaseRequestHandler requestHandler, [NotNull] string databaseName) 
        : base(requestHandler, databaseName)
    {
    }

    protected override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long durationInMs, long totalDocumentsSizeInBytes)
    {
        RequestHandler.AddPagingPerformanceHint(PagingOperationType.CompareExchange, action, details, numberOfResults, pageSize, durationInMs, totalDocumentsSizeInBytes);
    }

    internal class CompareExchangeListItem
    {
        public string Key { get; set; }
        public object Value { get; set; }
        public long Index { get; set; }
    }
}
