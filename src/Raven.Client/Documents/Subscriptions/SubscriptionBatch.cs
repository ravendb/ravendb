using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Sparrow.Logging;

namespace Raven.Client.Documents.Subscriptions
{
    public sealed class SubscriptionBatch<T> : SubscriptionBatchBase<T>
    {
        private readonly IDocumentStore _store;
        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        public SubscriptionBatch(RequestExecutor requestExecutor, IDocumentStore store, string dbName, RavenLogger logger) : base(requestExecutor, dbName, logger)
        {
            _store = store;
            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(_requestExecutor.Conventions, generateIdAsync: entity => throw new InvalidOperationException("Shouldn't be generating new ids here"));
        }


        private bool _sessionOpened;

        internal override ValueTask InitializeAsync(BatchFromServer batch)
        {
            _sessionOpened = false;

            return base.InitializeAsync(batch);
        }

        public IDocumentSession OpenSession()
        {
            return OpenSessionInternal(new SessionOptions
            {
                Database = _dbName,
                RequestExecutor = _requestExecutor
            });
        }

        public IDocumentSession OpenSession(SessionOptions options)
        {
            ValidateSessionOptions(options);

            options.Database = _dbName;
            options.RequestExecutor = _requestExecutor;

            return OpenSessionInternal(options);
        }

        private IDocumentSession OpenSessionInternal(SessionOptions options)
        {
            if (_sessionOpened)
            {
                ThrowSessionCanBeOpenedOnlyOnce();
            }
            _sessionOpened = true;
            var s = _store.OpenSession(options);

            LoadDataToSession((InMemoryDocumentSessionOperations)s);

            return s;
        }

        public IAsyncDocumentSession OpenAsyncSession()
        {
            return OpenAsyncSessionInternal(new SessionOptions
            {
                Database = _dbName,
                RequestExecutor = _requestExecutor
            });
        }

        public IAsyncDocumentSession OpenAsyncSession(SessionOptions options)
        {
            ValidateSessionOptions(options);

            options.Database = _dbName;
            options.RequestExecutor = _requestExecutor;

            return OpenAsyncSessionInternal(options);
        }

        private IAsyncDocumentSession OpenAsyncSessionInternal(SessionOptions options)
        {
            if (_sessionOpened)
            {
                ThrowSessionCanBeOpenedOnlyOnce();
            }
            _sessionOpened = true;

            var s = _store.OpenAsyncSession(options);

            LoadDataToSession((InMemoryDocumentSessionOperations)s);

            return s;
        }

        private static void ThrowSessionCanBeOpenedOnlyOnce()
        {
            throw new InvalidOperationException("Session can only be opened once per each Subscription batch");
        }

        private static void ValidateSessionOptions(SessionOptions options)
        {
            if (options.Database != null)
                throw new InvalidOperationException($"Cannot set '{nameof(options.Database)}' when session is opened in subscription.");

            if (options.RequestExecutor != null)
                throw new InvalidOperationException($"Cannot set '{nameof(options.RequestExecutor)}' when session is opened in subscription.");

            if (options.TransactionMode != TransactionMode.SingleNode)
                throw new InvalidOperationException($"Cannot set '{nameof(options.TransactionMode)}' when session is opened in subscription. Only '{nameof(TransactionMode.SingleNode)}' mode is supported.");
        }

        private void LoadDataToSession(InMemoryDocumentSessionOperations s)
        {
            if (s.NoTracking)
                return;

            if (_includes?.Count > 0)
            {
                foreach (var item in _includes)
                    s.RegisterIncludes(item, registerMissingIds: true);
            }

            if (_counterIncludes?.Count > 0)
            {
                foreach (var item in _counterIncludes)
                    s.RegisterCounters(item.Includes, item.IncludedCounterNames);
            }

            if (_timeSeriesIncludes?.Count > 0)
            {
                foreach (var item in _timeSeriesIncludes)
                    s.RegisterTimeSeries(item);
            }

            foreach (var item in Items)
            {
                if (item.Projection || item.Revision)
                    continue;

                s.RegisterExternalLoadedIntoTheSession(new DocumentInfo
                {
                    Id = item.Id,
                    Document = item.RawResult,
                    Metadata = item.RawMetadata,
                    MetadataInstance = item.Metadata,
                    ChangeVector = item.ChangeVector,
                    Entity = item.Result,
                    IsNewDocument = false
                });
            }
        }

        protected override void EnsureDocumentId(T item, string id) => _generateEntityIdOnTheClient.TrySetIdentity(item, id);
    }
}
