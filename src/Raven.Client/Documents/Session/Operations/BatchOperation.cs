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
        private int _allCommandsCount;

        private Dictionary<LazyStringValue, DocumentInfo> _modifications;

        public BatchCommand CreateRequest()
        {
            var result = _session.PrepareForSaveChanges();
            _sessionCommandsCount = result.SessionCommands.Count;

            result.SessionCommands.AddRange(result.DeferredCommands);
            _session.ValidateClusterTransaction(result);

            _allCommandsCount = result.SessionCommands.Count;

            if (_allCommandsCount == 0)
                return null;

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
                        HandlePut(i, batchResult, isDeferred: false);
                        break;
                    case CommandType.DELETE:
                        HandleDelete(batchResult);
                        break;
                    case CommandType.CompareExchangePUT:
                    case CommandType.CompareExchangeDELETE:
                        break;
                    default:
                        throw new NotSupportedException($"Command '{type}' is not supported.");
                }
            }

            for (var i = _sessionCommandsCount; i < _allCommandsCount; i++)
            {
                var batchResult = result.Results[i] as BlittableJsonReaderObject;
                if (batchResult == null)
                    continue;

                var type = GetCommandType(batchResult);

                switch (type)
                {
                    case CommandType.PUT:
                        HandlePut(i, batchResult, isDeferred: true);
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
                    case CommandType.BatchPATCH:
                        break;
                    default:
                        throw new NotSupportedException($"Command '{type}' is not supported.");
                }
            }

            FinalizeResult();
        }

        private void FinalizeResult()
        {
            if (_modifications == null || _modifications.Count == 0)
                return;

            foreach (var kvp in _modifications)
            {
                var id = kvp.Key;
                var documentInfo = kvp.Value;

                ApplyMetadataModifications(id, documentInfo);
            }
        }

        private void ApplyMetadataModifications(LazyStringValue id, DocumentInfo documentInfo)
        {
            if (documentInfo.Metadata.Modifications == null)
                return;

            documentInfo.MetadataInstance = null;

            using (documentInfo.Document)
            using (documentInfo.Metadata)
            {
                documentInfo.Metadata = _session.Context.ReadObject(documentInfo.Metadata, id);
                documentInfo.Metadata.Modifications = null;

                documentInfo.Document.Modifications = new DynamicJsonValue(documentInfo.Document)
                {
                    [Constants.Documents.Metadata.Key] = documentInfo.Metadata
                };

                documentInfo.Document = _session.Context.ReadObject(documentInfo.Document, id);
                documentInfo.Document.Modifications = null;
            }
        }

        private DocumentInfo GetOrAddModifications(LazyStringValue id, DocumentInfo documentInfo, bool applyModifications)
        {
            if (_modifications == null)
                _modifications = new Dictionary<LazyStringValue, DocumentInfo>(LazyStringValueComparer.Instance);

            if (_modifications.TryGetValue(id, out var modifiedDocumentInfo))
            {
                if (applyModifications)
                    ApplyMetadataModifications(id, modifiedDocumentInfo);
            }
            else
                _modifications[id] = modifiedDocumentInfo = documentInfo;

            return modifiedDocumentInfo;
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
            var id = GetLazyStringField(batchResult, type, idFieldName);

            if (_session.DocumentsById.TryGetValue(id, out var sessionDocumentInfo) == false)
                return;

            var documentInfo = GetOrAddModifications(id, sessionDocumentInfo, applyModifications: true);

            if (documentInfo.Metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachmentsJson) == false || attachmentsJson == null ||
                attachmentsJson.Length == 0)
                return;

            var name = GetLazyStringField(batchResult, type, attachmentNameFieldName);

            if (documentInfo.Metadata.Modifications == null)
                documentInfo.Metadata.Modifications = new DynamicJsonValue(documentInfo.Metadata);

            var attachments = new DynamicJsonArray();
            documentInfo.Metadata.Modifications[Constants.Documents.Metadata.Attachments] = attachments;

            foreach (BlittableJsonReaderObject attachment in attachmentsJson)
            {
                var attachmentName = GetLazyStringField(attachment, type, nameof(AttachmentDetails.Name));
                if (attachmentName == name)
                    continue;

                attachments.Add(attachment);
                break;
            }
        }

        private void HandleAttachmentPut(BlittableJsonReaderObject batchResult)
        {
            HandleAttachmentPutInternal(batchResult, CommandType.AttachmentPUT, nameof(PutAttachmentCommandData.Id), nameof(PutAttachmentCommandData.Name));
        }

        private void HandleAttachmentPutInternal(BlittableJsonReaderObject batchResult, CommandType type, string idFieldName, string attachmentNameFieldName)
        {
            var id = GetLazyStringField(batchResult, type, idFieldName);

            if (_session.DocumentsById.TryGetValue(id, out var sessionDocumentInfo) == false)
                return;

            var documentInfo = GetOrAddModifications(id, sessionDocumentInfo, applyModifications: false);

            if (documentInfo.Metadata.Modifications == null)
                documentInfo.Metadata.Modifications = new DynamicJsonValue(documentInfo.Metadata);

            var attachments = documentInfo.Metadata.Modifications[Constants.Documents.Metadata.Attachments] as DynamicJsonArray;
            if (attachments == null)
            {
                attachments = documentInfo.Metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachmentsJson)
                    ? new DynamicJsonArray(attachmentsJson)
                    : new DynamicJsonArray();

                documentInfo.Metadata.Modifications[Constants.Documents.Metadata.Attachments] = attachments;
            }

            attachments.Add(new DynamicJsonValue
            {
                [nameof(AttachmentDetails.ChangeVector)] = GetLazyStringField(batchResult, type, nameof(AttachmentDetails.ChangeVector)),
                [nameof(AttachmentDetails.ContentType)] = GetLazyStringField(batchResult, type, nameof(AttachmentDetails.ContentType)),
                [nameof(AttachmentDetails.Hash)] = GetLazyStringField(batchResult, type, nameof(AttachmentDetails.Hash)),
                [nameof(AttachmentDetails.Name)] = GetLazyStringField(batchResult, type, attachmentNameFieldName),
                [nameof(AttachmentDetails.Size)] = GetLongField(batchResult, type, nameof(AttachmentDetails.Size))
            });
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
                    if (batchResult.TryGet(nameof(PatchResult.ModifiedDocument), out BlittableJsonReaderObject document) == false)
                        return;

                    var id = GetLazyStringField(batchResult, CommandType.PATCH, nameof(ICommandData.Id));

                    if (_session.DocumentsById.TryGetValue(id, out var sessionDocumentInfo) == false)
                        return;

                    var documentInfo = GetOrAddModifications(id, sessionDocumentInfo, applyModifications: true);

                    var changeVector = GetLazyStringField(batchResult, CommandType.PATCH, nameof(Constants.Documents.Metadata.ChangeVector));
                    var lastModified = GetLazyStringField(batchResult, CommandType.PATCH, nameof(Constants.Documents.Metadata.LastModified));

                    documentInfo.ChangeVector = changeVector;

                    documentInfo.Metadata.Modifications = new DynamicJsonValue(documentInfo.Metadata)
                    {
                        [Constants.Documents.Metadata.Id] = id,
                        [Constants.Documents.Metadata.ChangeVector] = changeVector,
                        [Constants.Documents.Metadata.LastModified] = lastModified
                    };

                    using (var old = documentInfo.Document)
                    {
                        documentInfo.Document = document;

                        ApplyMetadataModifications(id, documentInfo);
                    }

                    if (documentInfo.Entity != null)
                        _session.EntityToBlittable.PopulateEntity(documentInfo.Entity, id, documentInfo.Document, _session.JsonSerializer);

                    break;
            }
        }

        private void HandleDelete(BlittableJsonReaderObject batchResult)
        {
            HandleDeleteInternal(batchResult, CommandType.DELETE);
        }

        private void HandleDeleteInternal(BlittableJsonReaderObject batchResult, CommandType type)
        {
            var id = GetLazyStringField(batchResult, type, nameof(ICommandData.Id));

            _modifications?.Remove(id);

            if (_session.DocumentsById.TryGetValue(id, out var documentInfo) == false)
                return;

            _session.DocumentsById.Remove(id);

            if (documentInfo.Entity != null)
            {
                _session.DocumentsByEntity.Remove(documentInfo.Entity);
                _session.DeletedEntities.Remove(documentInfo.Entity);
            }
        }

        private void HandlePut(int index, BlittableJsonReaderObject batchResult, bool isDeferred)
        {
            object entity = null;
            DocumentInfo documentInfo = null;
            if (isDeferred == false)
            {
                entity = _entities[index];
                if (_session.DocumentsByEntity.TryGetValue(entity, out documentInfo) == false)
                    return;
            }

            var id = GetLazyStringField(batchResult, CommandType.PUT, Constants.Documents.Metadata.Id);
            var changeVector = GetLazyStringField(batchResult, CommandType.PUT, Constants.Documents.Metadata.ChangeVector);

            if (isDeferred)
            {
                if (_session.DocumentsById.TryGetValue(id, out var sessionDocumentInfo) == false)
                    return;

                documentInfo = GetOrAddModifications(id, sessionDocumentInfo, applyModifications: true);

                entity = documentInfo.Entity;
            }

            documentInfo.Metadata.Modifications = new DynamicJsonValue(documentInfo.Metadata);

            foreach (var propertyName in batchResult.GetPropertyNames())
            {
                if (propertyName == nameof(ICommandData.Type))
                    continue;

                documentInfo.Metadata.Modifications[propertyName] = batchResult[propertyName];
            }

            documentInfo.Id = id;
            documentInfo.ChangeVector = changeVector;

            ApplyMetadataModifications(id, documentInfo);

            _session.DocumentsById.Add(documentInfo);

            if (entity != null)
                _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);

            var afterSaveChangesEventArgs = new AfterSaveChangesEventArgs(_session, documentInfo.Id, documentInfo.Entity);
            _session.OnAfterSaveChangesInvoke(afterSaveChangesEventArgs);
        }

        private void HandleCounters(BlittableJsonReaderObject batchResult)
        {
            var docId = GetLazyStringField(batchResult, CommandType.Counters, nameof(CountersBatchCommandData.Id));

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

        private static LazyStringValue GetLazyStringField(BlittableJsonReaderObject json, CommandType type, string fieldName)
        {
            if (json.TryGet(fieldName, out LazyStringValue value) == false || value == null)
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
