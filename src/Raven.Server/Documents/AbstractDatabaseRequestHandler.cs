using Raven.Server.ServerWide;
using Raven.Server.Web;
﻿using System.Threading.Tasks;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents;

public abstract class AbstractDatabaseRequestHandler<TOperationContext> : RequestHandler 
    where TOperationContext : JsonOperationContext
{
    public abstract string DatabaseName { get; }

    public Logger Logger;

    public abstract char IdentityPartsSeparator { get; }

    public abstract OperationCancelToken CreateTimeLimitedOperationToken();

    public JsonContextPoolBase<TOperationContext> ContextPool;

    public abstract Task WaitForIndexNotificationAsync(long index);

    public abstract bool ShouldAddPagingPerformanceHint(long numberOfResults);

    public abstract void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration, long totalDocumentsSizeInBytes);
}
