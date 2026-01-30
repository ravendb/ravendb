using System;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using System.Threading.Tasks;
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

    public abstract OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationToken();

    public abstract OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationTokenForQuery();

    public abstract OperationCancelToken CreateTimeLimitedBackgroundOperationTokenForQueryOperation();

    public abstract OperationCancelToken CreateTimeLimitedBackgroundOperationTokenForCollectionOperation();

    public abstract OperationCancelToken CreateTimeLimitedBackgroundOperationToken();

    public JsonContextPoolBase<TOperationContext> ContextPool;

    public abstract Task WaitForIndexNotificationAsync(long index);

    public abstract bool ShouldAddPagingPerformanceHint(long numberOfResults);

    public abstract void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, long pageSize, long duration, long totalDocumentsSizeInBytes);

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
