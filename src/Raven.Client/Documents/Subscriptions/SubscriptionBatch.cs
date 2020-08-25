using System;
using System.Collections.Generic;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionBatch<T>
    {
        /// <summary>
        /// Represents a single item in a subscription batch results. This class should be used only inside the subscription's Run delegate, using it outside this scope might cause unexpected behavior.
        /// </summary>
        public struct Item
        {
            private T _result;
            public string ExceptionMessage { get; internal set; }
            public string Id { get; internal set; }
            public string ChangeVector { get; internal set; }
            public bool Projection { get; internal set; }
            public bool Revision { get; internal set; }

            private void ThrowItemProcessException()
            {
                throw new InvalidOperationException($"Failed to process document {Id} with Change Vector {ChangeVector} because:{Environment.NewLine}{ExceptionMessage}");
            }

            public T Result
            {
                get
                {
                    if (ExceptionMessage != null)
                        ThrowItemProcessException();

                    return _result;
                }
                internal set => _result = value;
            }

            public BlittableJsonReaderObject RawResult { get; internal set; }
            public BlittableJsonReaderObject RawMetadata { get; internal set; }

            private IMetadataDictionary _metadata;
            public IMetadataDictionary Metadata => _metadata ?? (_metadata = new MetadataAsDictionary(RawMetadata));
        }

        public int NumberOfItemsInBatch => Items?.Count ?? 0;

        private readonly RequestExecutor _requestExecutor;
        private readonly IDocumentStore _store;
        private readonly string _dbName;
        private readonly Logger _logger;
        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        public List<Item> Items { get; } = new List<Item>();
        private List<BlittableJsonReaderObject> _includes;
        private List<(BlittableJsonReaderObject Includes, Dictionary<string, string[]> IncludedCounterNames)> _counterIncludes;
        private List<BlittableJsonReaderObject> _timeSeriesIncludes;

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
            var s = _store.OpenAsyncSession(options);

            LoadDataToSession((InMemoryDocumentSessionOperations)s);

            return s;
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
                    s.RegisterIncludes(item);
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
                    ChangeVector = item.ChangeVector,
                    Entity = item.Result,
                    IsNewDocument = false
                });
            }
        }

        public SubscriptionBatch(RequestExecutor requestExecutor, IDocumentStore store, string dbName, Logger logger)
        {
            _requestExecutor = requestExecutor;
            _store = store;
            _dbName = dbName;
            _logger = logger;

            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(_requestExecutor.Conventions, entity => throw new InvalidOperationException("Shouldn't be generating new ids here"));
        }

        internal string Initialize(BatchFromServer batch)
        {
            _includes = batch.Includes;
            _counterIncludes = batch.CounterIncludes;
            _timeSeriesIncludes = batch.TimeSeriesIncludes;

            Items.Capacity = Math.Max(Items.Capacity, batch.Messages.Count);
            Items.Clear();

            var revision = typeof(T).IsConstructedGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Revision<>);
            string lastReceivedChangeVector = null;

            foreach (var item in batch.Messages)
            {
                var curDoc = item.Data;

                if (curDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                    ThrowRequired("@metadata field");
                if (metadata.TryGet(Constants.Documents.Metadata.Id, out string id) == false)
                    ThrowRequired("@id field");
                if (metadata.TryGet(Constants.Documents.Metadata.ChangeVector, out string changeVector) == false ||
                    changeVector == null)
                    ThrowRequired("@change-vector field");
                else
                    lastReceivedChangeVector = changeVector;

                metadata.TryGet(Constants.Documents.Metadata.Projection, out bool projection);

                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Got {id} (change vector: [{lastReceivedChangeVector}], size {curDoc.Size}");
                }

                var instance = default(T);

                if (item.Exception == null)
                {
                    if (typeof(T) == typeof(BlittableJsonReaderObject))
                    {
                        instance = (T)(object)curDoc;
                    }
                    else
                    {
                        try
                        {
                            instance = _requestExecutor.Conventions.Serialization.DefaultConverter.FromBlittable<T>(curDoc, id);
                        }
                        catch (InvalidOperationException e)
                        {
                            throw new SubscriptionClosedException($"Could not serialize document '{id}' to '{typeof(T)}'. Closing the subscription.", e);
                        }
                    }

                    if (string.IsNullOrEmpty(id) == false)
                        _generateEntityIdOnTheClient.TrySetIdentity(instance, id);
                }

                Items.Add(new Item
                {
                    ChangeVector = changeVector,
                    Id = id,
                    RawResult = curDoc,
                    RawMetadata = metadata,
                    Result = instance,
                    ExceptionMessage = item.Exception,
                    Projection = projection,
                    Revision = revision
                });
            }
            return lastReceivedChangeVector;
        }

        private static void ThrowRequired(string name)
        {
            throw new InvalidOperationException("Document must have a " + name);
        }
    }
}
