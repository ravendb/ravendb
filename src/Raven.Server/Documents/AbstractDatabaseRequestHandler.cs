using System;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents;

public abstract class AbstractDatabaseRequestHandler<TOperationContext> : RequestHandler 
    where TOperationContext : JsonOperationContext
{
    public abstract string DatabaseName { get; }

    public RavenLogger Logger;
    public abstract RavenConfiguration Configuration { get; }
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

    public void LogAuditForDatabase(string action, string target, Exception e = null)
    {
        LogAuditForDatabase(DatabaseName, action, target, e);
    }

    public void LogAuditForIndex(string indexName, string action, string target, Exception e = null)
    {
        var auditLogger = RavenLogManager.Instance.GetAuditLoggerForIndex(DatabaseName, indexName);
        LogAuditForInternal(auditLogger, action, target, HttpContext, e);
    }

    private TOperationContext _context;

    public TOperationContext GetContextScopedToRequest()
    {
        if (_context == null)
        {
            RegisterForDisposal(ContextPool.AllocateOperationContext(out _context));
            RegisterForDisposal(new ContextCleanUp(this));
        }
        
        return _context;
    }

    /// <summary>
    /// Cleans up the context from the handler so that it's not misused.
    /// </summary>
    /// <remarks>
    /// This is just for asserting correctness.
    /// The context should never be used after the handler is disposed, so we should be able just to remove it if needed.
    /// </remarks>
    private sealed class ContextCleanUp(AbstractDatabaseRequestHandler<TOperationContext> handler) : IDisposable
    {
        public void Dispose()
        {
            handler._context = null;
        }
    }
}
