using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.Subscriptions;

/// <summary>
/// Base type for subscription batches, providing access to batch items, includes and helpers.
/// </summary>
public abstract class SubscriptionBatchBase<T>
{
    /// <summary>
    /// A single item in the batch, including the deserialized entity and its raw JSON and metadata.
    /// Use only within the subscription Run delegate.
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

        /// <summary>
        /// The deserialized entity. Accessing this property will throw if the item processing on the server failed.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when the server reported an exception for this item.</exception>
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

        /// <summary>
        /// Raw document JSON as received from the server.
        /// </summary>
        public BlittableJsonReaderObject RawResult { get; internal set; }
        /// <summary>
        /// Raw metadata JSON of the document.
        /// </summary>
        public BlittableJsonReaderObject RawMetadata { get; internal set; }

        /// <summary>
        /// A convenient metadata dictionary abstraction over the raw metadata.
        /// </summary>
        public IMetadataDictionary Metadata { get; internal set; }
    }

    /// <summary>
    /// The change vector of the last item in this batch.
    /// </summary>
    public string LastSentChangeVectorInBatch;
    /// <summary>
    /// The number of items in this batch.
    /// </summary>
    public int NumberOfItemsInBatch => Items?.Count ?? 0;
    internal int NumberOfIncludes => _includes?.Count ?? 0;

    protected readonly RequestExecutor _requestExecutor;
    protected readonly string _dbName;
    protected readonly IRavenLogger _logger;

    /// <summary>
    /// The items contained in this batch.
    /// </summary>
    public List<Item> Items { get; } = new List<Item>();
    protected List<BlittableJsonReaderObject> _includes;
    protected List<(BlittableJsonReaderObject Includes, Dictionary<string, string[]> IncludedCounterNames)> _counterIncludes;
    protected List<BlittableJsonReaderObject> _timeSeriesIncludes;

    protected SubscriptionBatchBase(RequestExecutor requestExecutor, string dbName, IRavenLogger logger)
    {
        _requestExecutor = requestExecutor;
        _dbName = dbName;
        _logger = logger;
    }

    protected abstract void EnsureDocumentId(T item, string id);

    internal virtual ValueTask InitializeAsync(BatchFromServer batch)
    {
        _includes = batch.Includes;
        _counterIncludes = batch.CounterIncludes;
        _timeSeriesIncludes = batch.TimeSeriesIncludes;

        Items.Capacity = Math.Max(Items.Capacity, batch.Messages.Count);
        Items.Clear();

        var revision = typeof(T).IsConstructedGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Revision<>);

        foreach (var item in batch.Messages)
        {
            var curDoc = item.Data;
            (BlittableJsonReaderObject metadata, string id, string changeVector) = BatchFromServer.GetMetadataFromBlittable(curDoc);
            LastSentChangeVectorInBatch = changeVector;
            metadata.TryGet(Constants.Documents.Metadata.Projection, out bool projection);

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Got {id} (change vector: [{changeVector}], size {curDoc.Size}");
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

                if (string.IsNullOrEmpty(id))
                    EnsureDocumentId(instance, id);
            }

            Items.Add(new Item
            {
                ChangeVector = changeVector,
                Id = id,
                RawResult = curDoc,
                RawMetadata = metadata,
                Metadata = new MetadataAsDictionary(metadata),
                Result = instance,
                ExceptionMessage = item.Exception,
                Projection = projection,
                Revision = revision
            });
        }

        return default;
    }
}
