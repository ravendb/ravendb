using System;
using System.Collections.Generic;
using Raven.NewClient.Client.Document;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class BatchOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<LoadOperation>("Raven.NewClient.Client");

        public BatchOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        protected void LogBatch()
        {
           //TODO
        }

        private List<object> _entities;
        private int _deferredCommandsCount;

        public BatchCommand CreateRequest()
        {
            var result = _session.PrepareForSaveChanges();
            _session.IncrementRequestCount();
            LogBatch();

            _entities = result.Entities;
            _deferredCommandsCount = result.DeferredCommandsCount;

            return new BatchCommand()
            {
                Commands = result.Commands,
                Context = _session.Context,
                Options = result?.Options,
                IsReadRequest = false
            };
        }

        public void SetResult(BlittableArrayResult result)
        {
            for (var i = _deferredCommandsCount; i < result.Results.Length; i++)
            {
                var batchResult = result.Results[i] as BlittableJsonReaderObject;
                if (batchResult == null)
                    throw new ArgumentNullException();

                string methodType;
                batchResult.TryGet("Method", out methodType);

                if (methodType != "PUT")
                    continue;

                var entity = _entities[i - _deferredCommandsCount];
                DocumentInfo documentInfo;

                if (_session.DocumentsByEntity.TryGetValue(entity, out documentInfo) == false)
                    continue;

                string key;
                long? etag;
                BlittableJsonReaderObject metadata;
                if (batchResult.TryGet("Metadata", out metadata))
                    documentInfo.Metadata = metadata;
                if (batchResult.TryGet("Etag", out etag))
                    documentInfo.ETag = etag;
                if (batchResult.TryGet("Key", out key))
                {
                    documentInfo.Id = key;
                    _session.DocumentsById[key] = documentInfo;
                }

                var afterStoreEventArgs = new AfterStoreEventArgs(_session, documentInfo.Id, documentInfo.Entity);
                _session.OnAfterStoreInvoke(afterStoreEventArgs);
            }
        }
    }
}