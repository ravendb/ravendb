using System;
using System.Collections.Generic;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Session;
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

        public IDocumentSession OpenSession()
        {
            var s = _store.OpenSession(new SessionOptions
            {
                Database = _dbName,
                RequestExecutor = _requestExecutor
            });

            LoadDataToSession((InMemoryDocumentSessionOperations)s);

            return s;
        }

        public IAsyncDocumentSession OpenAsyncSession()
        {
            var s = _store.OpenAsyncSession(new SessionOptions
            {
                Database = _dbName,
                RequestExecutor = _requestExecutor
            });
            LoadDataToSession((InMemoryDocumentSessionOperations)s);
            return s;
        }

        private void LoadDataToSession(InMemoryDocumentSessionOperations s)
        {
            if (_includes == null)
                return;

            foreach (var item in _includes)
            {
                s.RegisterIncludes(item);
            }
            foreach (var item in Items)
            {
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
            Items.Capacity = Math.Max(Items.Capacity, batch.Messages.Count);
            Items.Clear();
            string lastReceivedChangeVector = null;

            foreach (var item in batch.Messages)
            {

                BlittableJsonReaderObject metadata;
                var curDoc = item.Data;

                if (curDoc.TryGet(Constants.Documents.Metadata.Key, out metadata) == false)
                    ThrowRequired("@metadata field");
                if (metadata.TryGet(Constants.Documents.Metadata.Id, out string id) == false)
                    ThrowRequired("@id field");
                if (metadata.TryGet(Constants.Documents.Metadata.ChangeVector, out string changeVector) == false ||
                    changeVector == null)
                    ThrowRequired("@change-vector field");
                else
                    lastReceivedChangeVector = changeVector;

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
                        instance = (T)EntityToBlittable.ConvertToEntity(typeof(T), id, curDoc, _requestExecutor.Conventions);
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
                    ExceptionMessage = item.Exception
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
