using JetBrains.Annotations;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Web.System.Processors.CompareExchange;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.CompareExchange;

internal class ShardedCompareExchangeHandlerProcessorForGetCompareExchangeValues : AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues<ShardedDatabaseRequestHandler>
{
    public ShardedCompareExchangeHandlerProcessorForGetCompareExchangeValues([NotNull] ShardedDatabaseRequestHandler requestHandler, [NotNull] string databaseName) 
        : base(requestHandler, databaseName)
    {
    }

    protected override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long durationInMs, long totalDocumentsSizeInBytes)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Pawel, DevelopmentHelper.Severity.Minor, "Implement AddPagingPerformanceHint");
    }
}
