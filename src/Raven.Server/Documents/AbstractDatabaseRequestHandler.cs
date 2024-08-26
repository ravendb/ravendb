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

    public RavenLogger Logger;

    public abstract char IdentityPartsSeparator { get; }

    public abstract OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationToken();

    public abstract OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationTokenForQuery();

    public abstract OperationCancelToken CreateTimeLimitedBackgroundOperationTokenForQueryOperation();

    public abstract OperationCancelToken CreateTimeLimitedBackgroundOperationTokenForCollectionOperation();

    public abstract OperationCancelToken CreateTimeLimitedBackgroundOperationToken();

    public JsonContextPoolBase<TOperationContext> ContextPool;

    public abstract Task WaitForIndexNotificationAsync(long index);

    public abstract bool ShouldAddPagingPerformanceHint(long numberOfResults);

    public abstract void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, long pageSize, long duration, long totalDocumentsSizeInBytes);
}
