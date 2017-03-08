using System;
using System.Collections.Generic;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations
{
    internal class BatchOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;

        public BatchOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        private List<object> _entities;
        private int _deferredCommandsCount;

        public BatchCommand CreateRequest()
        {
            var result = _session.PrepareForSaveChanges();
            if (result.Commands.Count == 0)
                return null;

            _session.IncrementRequestCount();

            _entities = result.Entities;
            _deferredCommandsCount = result.DeferredCommandsCount;

            return new BatchCommand(_session.Conventions, _session.Context, result.Commands, result.Options);
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
                if (metadata.TryGet(Constants.Documents.Metadata.Etag, out etag))
                    documentInfo.ETag = etag;
                if (metadata.TryGet(Constants.Documents.Metadata.Id, out key))
                {
                    documentInfo.Id = key;
                    _session.DocumentsById.Add(documentInfo);
                    _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, key);
                }

                var afterStoreEventArgs = new AfterStoreEventArgs(_session, documentInfo.Id, documentInfo.Entity);
                _session.OnAfterStoreInvoke(afterStoreEventArgs);
            }
        }
    }
}