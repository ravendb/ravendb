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
        private int _sessionCommandsCount;

        public BatchCommand CreateRequest()
        {
            var result = _session.PrepareForSaveChanges();
            _sessionCommandsCount = result.SessionCommands.Count;
            result.SessionCommands.AddRange(result.DeferredCommands);
            if (result.SessionCommands.Count == 0)
                return null;

            _session.IncrementRequestCount();

            _entities = result.Entities;

            return new BatchCommand(_session.Conventions, _session.Context, result.SessionCommands, result.Options);
        }

        public void SetResult(BlittableArrayResult result)
        {
            if (result.Results == null) //precaution
            {
                ThrowOnNullResults();
                return;
            }

            for (var i = 0; i < _sessionCommandsCount; i++)
            {
                var batchResult = result.Results[i] as BlittableJsonReaderObject;
                if (batchResult == null)
                    throw new ArgumentNullException();

                batchResult.TryGet("Type", out string type);

                if (type != "PUT")
                    continue;

                var entity = _entities[i];
                if (_session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo documentInfo) == false)
                    continue;

                if (batchResult.TryGet(Constants.Documents.Metadata.Etag, out long? etag) == false || etag == null)
                    throw new InvalidOperationException("PUT response is invalid. @etag is missing.");

                if (batchResult.TryGet(Constants.Documents.Metadata.Id, out string key) == false || key == null)
                    throw new InvalidOperationException("PUT response is invalid. @id is missing.");

                documentInfo.Metadata.Modifications = null;
                documentInfo.Metadata.Modifications = new DynamicJsonValue(documentInfo.Metadata);

                foreach (var propertyName in batchResult.GetPropertyNames())
                {
                    if(propertyName == "Type")
                        continue;

                    documentInfo.Metadata.Modifications[propertyName] = batchResult[propertyName];
                }

                documentInfo.Id = key;
                documentInfo.ETag = etag;
                documentInfo.Metadata = _session.Context.ReadObject(documentInfo.Metadata, key);
                documentInfo.Document.Modifications = null;
                documentInfo.Document.Modifications = new DynamicJsonValue(documentInfo.Document)
                {
                    [Constants.Documents.Metadata.Key] = documentInfo.Metadata
                };
                documentInfo.Document = _session.Context.ReadObject(documentInfo.Document, key);
                documentInfo.MetadataInstance = null;

                _session.DocumentsById.Add(documentInfo);
                _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, key);

                var afterStoreEventArgs = new AfterStoreEventArgs(_session, documentInfo.Id, documentInfo.Entity);
                _session.OnAfterStoreInvoke(afterStoreEventArgs);
            }
        }

        private static void ThrowOnNullResults()
        {
            throw new InvalidOperationException("Received empty response from the server. This is not supposed to happen and is likely a bug.");
        }
    }
}