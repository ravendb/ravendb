using System;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.AspNetCore.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow.Server.Utils;

namespace Raven.Server.Web
{
    public sealed class RequestHandlerContext : IDisposable
    {
        // It uses inline disposables instead of DisposableScope to save ~64bytes.
        private IDisposable[] _disposables;
        private int _disposableCount;

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
            _disposables ??= new IDisposable[4];

            if (_disposableCount >= _disposables.Length)
            {
                Array.Resize(ref _disposables, _disposables.Length * 2);
            }

            _disposables[_disposableCount++] = disposable;
        }

        public void Dispose()
        {
            if (_disposables == null)
                return;

            List<Exception> errors = null;

            while (_disposableCount >= 1)
            {
                _disposableCount--;

                IDisposable disposable = _disposables[_disposableCount];
                try
                {
                    disposable.Dispose();
                }
                catch (Exception e)
                {
                    errors ??= [];
                    errors.Add(e);
                }
            }

            _disposables = null;

            if (errors != null)
                throw new AggregateException(errors);
        }
    }
}
