using System;
using System.Collections.Generic;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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

                long? etag;
                if (batchResult.TryGet("Etag", out etag) == false || etag == null)
                    throw new InvalidOperationException("PUT response is invalid. Etag is missing.");

                string key;
                if (batchResult.TryGet("Key", out key) == false || key == null)
                    throw new InvalidOperationException("PUT response is invalid. Key is missing.");

                string collection;
                if (batchResult.TryGet("Collection", out collection) == false || collection == null)
                    throw new InvalidOperationException("PUT response is invalid. Collection is missing.");

                BlittableJsonReaderArray changeVectorArray;
                if (batchResult.TryGet("ChangeVector", out changeVectorArray) == false || changeVectorArray == null)
                    throw new InvalidOperationException("PUT response is invalid. ChangeVector is missing.");

                if (documentInfo.Metadata.Modifications == null)
                    documentInfo.Metadata.Modifications = new DynamicJsonValue();

                documentInfo.Metadata.Modifications[Constants.Documents.Metadata.Id] = key;
                documentInfo.Metadata.Modifications[Constants.Documents.Metadata.Etag] = etag;
                documentInfo.Metadata.Modifications[Constants.Documents.Metadata.Collection] = collection;
                documentInfo.Metadata.Modifications[Constants.Documents.Metadata.ChangeVector] = changeVectorArray;

                documentInfo.Id = key;
                documentInfo.ETag = etag;
                documentInfo.Metadata = _session.Context.ReadObject(documentInfo.Metadata, key);
                documentInfo.MetadataInstance = null;

                _session.DocumentsById.Add(documentInfo);
                _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, key);

                var afterStoreEventArgs = new AfterStoreEventArgs(_session, documentInfo.Id, documentInfo.Entity);
                _session.OnAfterStoreInvoke(afterStoreEventArgs);
            }
        }
    }
}