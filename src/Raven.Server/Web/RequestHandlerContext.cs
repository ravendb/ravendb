using System;
using Microsoft.AspNetCore.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow.Server.Utils;

namespace Raven.Server.Web
{
    public sealed class RequestHandlerContext
    {
        private static readonly DisposableScope DisposedSentinel = new();
        private DisposableScope _disposables;

        public HttpContext HttpContext;
        public RavenServer RavenServer;
        public RouteMatch RouteMatch;
        public DocumentDatabase Database;
        public bool CheckForChanges = true;
        public ShardedDatabaseContext DatabaseContext;

        public string DatabaseName => Database?.Name ?? DatabaseContext?.DatabaseName;
        public MetricCounters DatabaseMetrics => Database?.Metrics ?? DatabaseContext?.Metrics;
        public string ClusterTransactionId => Database?.ClusterTransactionId ?? DatabaseContext?.DatabaseRecord.GetClusterTransactionId();
        
        public void RegisterForDisposal(IDisposable disposable)
        {
            if (ReferenceEquals(_disposables, DisposedSentinel))
                throw new ObjectDisposedException(nameof(RequestHandlerContext));

            var disposables = _disposables;
            if (disposables == null)
            {
                disposables = new DisposableScope();
                _disposables = disposables;
            }

            disposables.EnsureDispose(disposable);
        }

        public void Dispose()
        {
            var disposables = _disposables;
           
            if (ReferenceEquals(disposables, DisposedSentinel))
                return;

            _disposables = DisposedSentinel;

            disposables?.Dispose();
        }
    }
}
