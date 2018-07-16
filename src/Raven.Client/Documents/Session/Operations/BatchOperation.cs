using System;
using System.Collections.Generic;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions;
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
            result.SessionCommands.AddRange(result.DeferredCommands);

            _sessionCommandsCount = result.SessionCommands.Count;
            if (_sessionCommandsCount == 0)
                return null;

            _session.ValidateClusterTransaction(result);

            _session.IncrementRequestCount();

            _entities = result.Entities;

            return new BatchCommand(_session.Conventions, _session.Context, result.SessionCommands, result.Options, _session.TransactionMode);
        }

        public void SetResult(BatchCommandResult result)
        {
            CommandType GetCommandType(BlittableJsonReaderObject batchResult)
            {
                if (batchResult.TryGet(nameof(ICommandData.Type), out string typeAsString) == false)
                    return CommandType.None;

                if (Enum.TryParse(typeAsString, ignoreCase: true, out CommandType type) == false)
                    return CommandType.None;

                return type;
            }

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
                    continue;

                var type = GetCommandType(batchResult);

                switch (type)
                {
                    case CommandType.PUT:
                        HandlePut(i, batchResult);
                        break;
                    case CommandType.DELETE:
                        HandleDelete(batchResult);
                        break;
                    case CommandType.PATCH:
                        HandlePatch(batchResult);
                        break;
                    case CommandType.AttachmentPUT:
                        HandleAttachmentPut(batchResult);
                        break;
                    case CommandType.AttachmentDELETE:
                        HandleAttachmentDelete(batchResult);
                        break;
                    case CommandType.AttachmentMOVE:
                        HandleAttachmentMove(batchResult);
                        break;
                    case CommandType.AttachmentCOPY:
                        HandleAttachmentCopy(batchResult);
                        break;
                    case CommandType.CompareExchangePUT:
                    case CommandType.CompareExchangeDELETE:
                        break;
                    case CommandType.Counters:
                        HandleCounters(batchResult);
                        break;
                    default:
                        throw new NotSupportedException($"Command '{type}' is not supported.");
                }
            }
        }

        private void HandleAttachmentCopy(BlittableJsonReaderObject batchResult)
        {
            HandleAttachmentPutInternal(batchResult, CommandType.AttachmentCOPY, nameof(CopyAttachmentCommandData.Id), nameof(CopyAttachmentCommandData.Name));
        }

        private void HandleAttachmentMove(BlittableJsonReaderObject batchResult)
        {
            HandleAttachmentDeleteInternal(batchResult, CommandType.AttachmentMOVE, nameof(MoveAttachmentCommandData.Id), nameof(MoveAttachmentCommandData.Name));
            HandleAttachmentPutInternal(batchResult, CommandType.AttachmentMOVE, nameof(MoveAttachmentCommandData.DestinationId), nameof(MoveAttachmentCommandData.DestinationName));
        }

        private void HandleAttachmentDelete(BlittableJsonReaderObject batchResult)
        {
            HandleAttachmentDeleteInternal(batchResult, CommandType.AttachmentDELETE, Constants.Documents.Metadata.Id, nameof(DeleteAttachmentCommandData.Name));
        }

        private void HandleAttachmentDeleteInternal(BlittableJsonReaderObject batchResult, CommandType type, string idFieldName, string attachmentNameFieldName)
        {
            var id = GetStringField(batchResult, type, idFieldName);

            if (_session.DocumentsById.TryGetValue(id, out var documentInfo) == false)
                return;

            if (documentInfo.Metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachmentsJson) == false || attachmentsJson == null ||
                attachmentsJson.Length == 0)
                return;

            var name = GetStringField(batchResult, type, attachmentNameFieldName);

            documentInfo.Metadata.Modifications = null;
            documentInfo.Metadata.Modifications = new DynamicJsonValue(documentInfo.Metadata);

            var attachments = new DynamicJsonArray();
            documentInfo.Metadata.Modifications[Constants.Documents.Metadata.Attachments] = attachments;

            foreach (BlittableJsonReaderObject attachment in attachmentsJson)
            {
                var attachmentName = GetStringField(attachment, type, nameof(AttachmentDetails.Name));
                if (string.Equals(attachmentName, name))
                    continue;

                attachments.Add(attachment);
                break;
            }

            documentInfo.MetadataInstance = null;
            documentInfo.Metadata = _session.Context.ReadObject(documentInfo.Metadata, id);
        }

        private void HandleAttachmentPut(BlittableJsonReaderObject batchResult)
        {
            HandleAttachmentPutInternal(batchResult, CommandType.AttachmentPUT, nameof(PutAttachmentCommandData.Id), nameof(PutAttachmentCommandData.Name));
        }

        private void HandleAttachmentPutInternal(BlittableJsonReaderObject batchResult, CommandType type, string idFieldName, string attachmentNameFieldName)
        {
            var id = GetStringField(batchResult, type, idFieldName);

            if (_session.DocumentsById.TryGetValue(id, out var documentInfo) == false)
                return;

            documentInfo.Metadata.Modifications = null;
            documentInfo.Metadata.Modifications = new DynamicJsonValue(documentInfo.Metadata);

            var attachments = documentInfo.Metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachmentsJson)
                ? new DynamicJsonArray(attachmentsJson)
                : new DynamicJsonArray();

            documentInfo.Metadata.Modifications[Constants.Documents.Metadata.Attachments] = attachments;

            attachments.Add(new DynamicJsonValue
            {
                [nameof(AttachmentDetails.ChangeVector)] = GetStringField(batchResult, type, nameof(AttachmentDetails.ChangeVector)),
                [nameof(AttachmentDetails.ContentType)] = GetStringField(batchResult, type, nameof(AttachmentDetails.ContentType)),
                [nameof(AttachmentDetails.Hash)] = GetStringField(batchResult, type, nameof(AttachmentDetails.Hash)),
                [nameof(AttachmentDetails.Name)] = GetStringField(batchResult, type, attachmentNameFieldName),
                [nameof(AttachmentDetails.Size)] = GetLongField(batchResult, type, nameof(AttachmentDetails.Size))
            });

            documentInfo.MetadataInstance = null;
            documentInfo.Metadata = _session.Context.ReadObject(documentInfo.Metadata, id);
        }

        private void HandlePatch(BlittableJsonReaderObject batchResult)
        {
            if (batchResult.TryGet(nameof(PatchStatus), out string statusAsString) == false)
                ThrowMissingField(CommandType.PATCH, nameof(PatchStatus));

            if (Enum.TryParse(statusAsString, ignoreCase: true, out PatchStatus status) == false)
                ThrowMissingField(CommandType.PATCH, nameof(PatchStatus));

            switch (status)
            {
                case PatchStatus.Created:
                case PatchStatus.Patched:
                    HandleDeleteInternal(batchResult, CommandType.PATCH); // deleting because it is impossible to know what was in the patch
                    break;
            }
        }

        private void HandleDelete(BlittableJsonReaderObject batchResult)
        {
            HandleDeleteInternal(batchResult, CommandType.DELETE);
        }

        private void HandleDeleteInternal(BlittableJsonReaderObject batchResult, CommandType type)
        {
            var id = GetStringField(batchResult, type, nameof(ICommandData.Id));

            if (_session.DocumentsById.TryGetValue(id, out var documentInfo) == false)
                return;

            _session.DocumentsById.Remove(id);

            if (documentInfo.Entity != null)
            {
                _session.DocumentsByEntity.Remove(documentInfo.Entity);
                _session.DeletedEntities.Remove(documentInfo.Entity);
            }
        }

        private void HandlePut(int index, BlittableJsonReaderObject batchResult)
        {
            var entity = _entities[index];
            if (_session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo documentInfo) == false)
                return;

            var id = GetStringField(batchResult, CommandType.PUT, Constants.Documents.Metadata.Id);
            var changeVector = GetStringField(batchResult, CommandType.PUT, Constants.Documents.Metadata.ChangeVector);

            documentInfo.Metadata.Modifications = null;
            documentInfo.Metadata.Modifications = new DynamicJsonValue(documentInfo.Metadata);

            foreach (var propertyName in batchResult.GetPropertyNames())
            {
                if (propertyName == nameof(ICommandData.Type))
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

        private void HandleCounters(BlittableJsonReaderObject batchResult)
        {
            var docId = GetStringField(batchResult, CommandType.Counters, nameof(CountersBatchCommandData.Id));

            if (batchResult.TryGet(nameof(CountersDetail), out BlittableJsonReaderObject countersDetail) == false)
                ThrowMissingField(CommandType.Counters, nameof(CountersDetail));

            if (countersDetail.TryGet(nameof(CountersDetail.Counters), out BlittableJsonReaderArray counters) == false)
                ThrowMissingField(CommandType.Counters, nameof(CountersDetail.Counters));

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

        private static string GetStringField(BlittableJsonReaderObject json, CommandType type, string fieldName)
        {
            if (json.TryGet(fieldName, out string value) == false || value == null)
                ThrowMissingField(type, fieldName);

            return value;
        }

        private static long GetLongField(BlittableJsonReaderObject json, CommandType type, string fieldName)
        {
            if (json.TryGet(fieldName, out long longValue) == false)
                ThrowMissingField(type, fieldName);

            return longValue;
        }

        private static void ThrowMissingField(CommandType type, string fieldName)
        {
            throw new InvalidOperationException($"{type} response is invalid. Field '{fieldName}' is missing.");
        }

        private static void ThrowOnNullResults()
        {
            throw new InvalidOperationException("Received empty response from the server. This is not supposed to happen and is likely a bug.");
        }
    }
}
