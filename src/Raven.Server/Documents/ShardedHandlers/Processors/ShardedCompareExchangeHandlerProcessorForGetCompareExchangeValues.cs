using JetBrains.Annotations;
using Raven.Server.Documents.Sharding;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Web.System.Processors;
using Sparrow.Utils;

namespace Raven.Server.Documents.ShardedHandlers.Processors;

public class ShardedCompareExchangeHandlerProcessorForGetCompareExchangeValues : AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues
{
    public ShardedCompareExchangeHandlerProcessorForGetCompareExchangeValues([NotNull] ShardedRequestHandler requestHandler, [NotNull] string databaseName) : base(requestHandler, databaseName)
    {
    }

    protected override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long durationInMs, long totalDocumentsSizeInBytes)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Pawel, DevelopmentHelper.Severity.Minor, "Implement AddPagingPerformanceHint");
    }
}
