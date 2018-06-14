using System;
using System.Collections.Generic;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
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

            _session.ValidateClusterTransaction(result);

            _session.IncrementRequestCount();

            _entities = result.Entities;

            return new BatchCommand(_session.Conventions, _session.Context, result.SessionCommands, result.Options, _session.TransactionMode);
        }

        public void SetResult(BatchCommandResult result)
        {
            if (result.Results == null) //precaution
            {
                ThrowOnNullResults();
                return;
            }

            if (_session.TransactionMode == TransactionMode.ClusterWide)
            {
                if (result.TransactionIndex <= 0)
                    throw new ClientVersionMismatchException(
                        $"Cluster transaction was send to a node that is not supporting it. So it was executed ONLY on the requested node on {_session.RequestExecutor.Url}.");
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

                if (batchResult.TryGet(Constants.Documents.Metadata.ChangeVector, out string changeVector) == false || changeVector == null)
                    throw new InvalidOperationException("PUT response is invalid. @change-vector is missing on " + documentInfo.Id);

                if (batchResult.TryGet(Constants.Documents.Metadata.Id, out string id) == false || id == null)
                    throw new InvalidOperationException("PUT response is invalid. @id is missing on " + documentInfo.Id);

                documentInfo.Metadata.Modifications = null;
                documentInfo.Metadata.Modifications = new DynamicJsonValue(documentInfo.Metadata);

                foreach (var propertyName in batchResult.GetPropertyNames())
                {
                    if(propertyName == "Type")
                        continue;

                    documentInfo.Metadata.Modifications[propertyName] = batchResult[propertyName];
                }

                documentInfo.Id = id;
                documentInfo.ChangeVector = changeVector;
                documentInfo.Metadata = _session.Context.ReadObject(documentInfo.Metadata, id);
                documentInfo.Document.Modifications = null;
                documentInfo.Document.Modifications = new DynamicJsonValue(documentInfo.Document)
                {
                    [Constants.Documents.Metadata.Key] = documentInfo.Metadata
                };
                documentInfo.Document = _session.Context.ReadObject(documentInfo.Document, id);
                documentInfo.MetadataInstance = null;

                _session.DocumentsById.Add(documentInfo);
                _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);

                var afterSaveChangesEventArgs = new AfterSaveChangesEventArgs(_session, documentInfo.Id, documentInfo.Entity);
                _session.OnAfterSaveChangesInvoke(afterSaveChangesEventArgs);
            }

            for (int i = _sessionCommandsCount; i < result.Results.Length; i++)
            {
                if (!(result.Results[i] is BlittableJsonReaderObject batchResult))
                    throw new ArgumentNullException();

                if (batchResult.TryGet(nameof(CountersDetail), out BlittableJsonReaderObject countersDetail) == false ||
                    batchResult.TryGet(nameof(CountersBatchCommandData.Id), out string docId) == false ||
                    countersDetail.TryGet("Counters", out BlittableJsonReaderArray counters) == false)
                    continue;

                UpdateCounterValuesInCache(counters, docId);

            }

        }

        private void UpdateCounterValuesInCache(BlittableJsonReaderArray counters, string docId)
        {
            if (_session.CountersByDocId.TryGetValue(docId, out var cache) == false)
            {
                cache.Values = new Dictionary<string, long?>(StringComparer.OrdinalIgnoreCase);
                _session.CountersByDocId.Add(docId, cache);
            }

            foreach (BlittableJsonReaderObject counter in counters)
            {
                if (counter.TryGet(nameof(CounterDetail.CounterName), out string name) == false ||
                    counter.TryGet(nameof(CounterDetail.TotalValue), out long value) == false)
                    continue;

                cache.Values[name] = value;
            }
        }

        private static void ThrowOnNullResults()
        {
            throw new InvalidOperationException("Received empty response from the server. This is not supposed to happen and is likely a bug.");
        }
    }
}
