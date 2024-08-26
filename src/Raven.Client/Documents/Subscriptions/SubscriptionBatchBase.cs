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

public abstract class SubscriptionBatchBase<T>
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

        public IMetadataDictionary Metadata { get; internal set; }
    }

    public string LastSentChangeVectorInBatch;
    public int NumberOfItemsInBatch => Items?.Count ?? 0;
    internal int NumberOfIncludes => _includes?.Count ?? 0;

    protected readonly RequestExecutor _requestExecutor;
    protected readonly string _dbName;
    protected readonly RavenLogger _logger;

    public List<Item> Items { get; } = new List<Item>();
    protected List<BlittableJsonReaderObject> _includes;
    protected List<(BlittableJsonReaderObject Includes, Dictionary<string, string[]> IncludedCounterNames)> _counterIncludes;
    protected List<BlittableJsonReaderObject> _timeSeriesIncludes;

    protected SubscriptionBatchBase(RequestExecutor requestExecutor, string dbName, RavenLogger logger)
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
