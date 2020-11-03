// -----------------------------------------------------------------------
//  <copyright file="CountersHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Counters;
using Raven.Client.Json.Serialization;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;

namespace Raven.Server.Documents.Handlers
{
    public class CountersHandler : DatabaseRequestHandler
    {
        public class ExecuteCounterBatchCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public bool HasWrites;
            public string LastChangeVector;
            public string LastDocumentChangeVector;

            public CountersDetail CountersDetail = new CountersDetail
            {
                Counters = new List<CounterDetail>()
            };

            private readonly DocumentDatabase _database;
            private readonly bool _replyWithAllNodesValues;
            private readonly bool _fromEtl;
            private readonly List<CounterOperation> _list;

            public ExecuteCounterBatchCommand(DocumentDatabase database, CounterBatch counterBatch)
            {
                _database = database;
                _list = new List<CounterOperation>();
                _replyWithAllNodesValues = counterBatch?.ReplyWithAllNodesValues ?? false;
                _fromEtl = counterBatch?.FromEtl ?? false;

                if (counterBatch == null)
                    return;

                foreach (var docOps in counterBatch.Documents)
                {
                    foreach (var operation in docOps.Operations)
                    {
                        HasWrites |= operation.Type != CounterOperationType.Get &&
                                     operation.Type != CounterOperationType.None;

                        operation.DocumentId = docOps.DocumentId;
                        _list.Add(operation);
                    }
                }
            }

            /// <summary>
            /// Used only from replay Tx commands
            /// </summary>
            public ExecuteCounterBatchCommand(
                DocumentDatabase database,
                List<CounterOperation> list,
                bool replyWithAllNodesValues,
                bool fromEtl)
            {
                _database = database;
                _replyWithAllNodesValues = replyWithAllNodesValues;
                _fromEtl = fromEtl;
                _list = list;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var countersToAdd = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                var countersToRemove = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                Document doc = null;
                string docId = null;
                string docCollection = null;

                foreach (var operation in _list)
                {
                    switch (operation.Type)
                    {
                        case CounterOperationType.Increment:
                        case CounterOperationType.Delete:
                        case CounterOperationType.Put:
                            LoadDocument(operation);
                            break;
                    }

                    docId = operation.DocumentId;

                    switch (operation.Type)
                    {
                        case CounterOperationType.Increment:
                            LastChangeVector =
                                _database.DocumentsStorage.CountersStorage.IncrementCounter(context, docId, docCollection, operation.CounterName, operation.Delta, out var exists);
                            GetCounterValue(context, _database, docId, operation.CounterName, _replyWithAllNodesValues, CountersDetail, capValueOnOverflow: operation.Delta < 0);

                            if (exists == false)
                            {
                                // if exists it is already on the document's metadata
                                countersToAdd.Add(operation.CounterName);
                                countersToRemove.Remove(operation.CounterName);
                            }
                            break;

                        case CounterOperationType.Delete:
                            if (_fromEtl && doc == null)
                                break;

                            LastChangeVector = _database.DocumentsStorage.CountersStorage.DeleteCounter(context, docId, docCollection, operation.CounterName);

                            countersToAdd.Remove(operation.CounterName);
                            countersToRemove.Add(operation.CounterName);
                            break;

                        case CounterOperationType.Put:
                            if (_fromEtl == false || doc == null)
                                break;

                            _database.DocumentsStorage.CountersStorage.PutCounter(context, docId, docCollection, operation.CounterName, operation.Delta);

                            countersToAdd.Add(operation.CounterName);
                            countersToRemove.Remove(operation.CounterName);
                            break;

                        case CounterOperationType.None:
                            break;

                        case CounterOperationType.Get:
                            GetCounterValue(context, _database, docId, operation.CounterName, _replyWithAllNodesValues, CountersDetail);
                            break;

                        default:
                            ThrowInvalidBatchOperationType(operation);
                            break;
                    }
                }

                if (doc?.Data != null)
                {
                    var changeVector = _database
                        .DocumentsStorage
                        .CountersStorage
                        .UpdateDocumentCounters(context, doc, docId, countersToAdd, countersToRemove, NonPersistentDocumentFlags.ByCountersUpdate);

                    if (changeVector != null)
                        LastDocumentChangeVector = LastChangeVector = changeVector;

                    doc.Data.Dispose(); // we cloned the data, so we can dispose it.
                }

                countersToAdd.Clear();
                countersToRemove.Clear();

                void LoadDocument(CounterOperation counterOperation)
                {
                    if (string.IsNullOrEmpty(counterOperation.DocumentId))
                        throw new ArgumentException("Document ID can't be null");

                    if (docId == counterOperation.DocumentId && doc != null)
                        return;

                    ApplyChangesForPreviousDocument(context, doc, docId, countersToAdd, countersToRemove);

                    docId = counterOperation.DocumentId;

                    docCollection = GetDocumentCollection(docId, _database, context, _fromEtl, out doc);
                }

                ApplyChangesForPreviousDocument(context, doc, docId, countersToAdd, countersToRemove);

                return _list.Count;
            }

            public static string GetDocumentCollection(string docId, DocumentDatabase documentDatabase, DocumentsOperationContext context, bool fromEtl, out Document doc)
            {
                try
                {
                    doc = documentDatabase.DocumentsStorage.Get(context, docId, throwOnConflict: true);
                    if (doc == null)
                    {
                        if (fromEtl)
                            return null;

                        ThrowMissingDocument(docId);
                        return null; // never hit
                    }

                    if (doc.Flags.HasFlag(DocumentFlags.Artificial))
                        ThrowArtificialDocument(doc);

                    return CollectionName.GetCollectionName(doc.Data);
                }
                catch (DocumentConflictException)
                {
                    doc = null;

                    if (fromEtl)
                        return null;

                    // this is fine, we explicitly support
                    // setting the flag if we are in conflicted state is
                    // done by the conflict resolver

                    // avoid loading same document again, we validate write using the metadata instance
                    doc = new Document();
                    return documentDatabase.DocumentsStorage.ConflictsStorage.GetCollection(context, docId);
                }
            }

            private void ApplyChangesForPreviousDocument(DocumentsOperationContext context, Document doc, string docId, SortedSet<string> countersToAdd, HashSet<string> countersToRemove)
            {
                if (doc?.Data != null)
                {
                    var nonPersistentFlags = NonPersistentDocumentFlags.ByCountersUpdate;

                    _database.DocumentsStorage.CountersStorage.UpdateDocumentCounters(context, doc, docId, countersToAdd, countersToRemove, nonPersistentFlags);
                    doc.Data.Dispose(); // we cloned the data, so we can dispose it.
                }
                countersToAdd.Clear();
                countersToRemove.Clear();
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new ExecuteCounterBatchCommandDto
                {
                    List = _list,
                    ReplyWithAllNodesValues = _replyWithAllNodesValues,
                    FromEtl = _fromEtl
                };
            }

            public static void ThrowArtificialDocument(Document doc)
            {
                throw new InvalidOperationException($"Document '{doc.Id}' has '{nameof(DocumentFlags.Artificial)}' flag set. " +
                                                    "Cannot put Counters on artificial documents.");
            }

            private static void ThrowInvalidBatchOperationType(CounterOperation operation)
            {
                throw new ArgumentOutOfRangeException($"Unknown value {operation.Type}");
            }
        }

        public class SmugglerCounterBatchCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
        {
            private readonly DocumentDatabase _database;
            private readonly List<CounterGroupDetail> _counterGroups;
            private Dictionary<string, Dictionary<string, List<(string ChangeVector, long Value)>>> _legacyDictionary;

            private Dictionary<string, Document> _counterUpdates;

            private readonly DocumentsOperationContext _context;

            private IDisposable _resetContext;
            private bool _isDisposed;

            private readonly List<IDisposable> _toDispose;

            private SmugglerResult _result;

            public DocumentsOperationContext Context => _context;

            public long ErrorCount;

            public SmugglerCounterBatchCommand(DocumentDatabase database, SmugglerResult result)
            {
                _database = database;
                _result = result;
                _counterGroups = new List<CounterGroupDetail>();
                _counterUpdates = new Dictionary<string, Document>();

                _toDispose = new List<IDisposable>();
                _resetContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            }

            /// <summary>
            /// Used only from replay Tx commands
            /// </summary>
            public SmugglerCounterBatchCommand(
                DocumentDatabase database,
                List<CounterGroupDetail> counterGroups,
                Dictionary<string, Dictionary<string, List<(string ChangeVector, long Value)>>> legacyDictionary)
            {
                _database = database;
                _counterGroups = counterGroups;
                _legacyDictionary = legacyDictionary;

                _toDispose = new List<IDisposable>();
                _resetContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            }

            public void Add(CounterGroupDetail cgd)
            {
                _counterGroups.Add(cgd);
            }

            public void AddLegacy(string id, CounterDetail counterDetail)
            {
                _legacyDictionary ??= new Dictionary<string, Dictionary<string, List<(string ChangeVector, long Value)>>>(StringComparer.OrdinalIgnoreCase);
                var valueToAdd = (counterDetail.ChangeVector, counterDetail.TotalValue);

                if (_legacyDictionary.TryGetValue(counterDetail.DocumentId, out var counters))
                {
                    if (counters.TryGetValue(counterDetail.CounterName, out var counterValues))
                    {
                        counterValues.Add(valueToAdd);
                    }
                    else
                    {
                        counters.Add(counterDetail.CounterName, new List<(string ChangeVector, long Value)>
                        {
                            valueToAdd
                        });
                    }
                }
                else
                {
                    _legacyDictionary[counterDetail.DocumentId] = new Dictionary<string, List<(string ChangeVector, long Value)>>
                    {
                        {
                            counterDetail.CounterName, new List<(string ChangeVector, long Value)>
                            {
                                valueToAdd
                            }
                        }
                    };
                }
            }

            public void RegisterForDisposal(IDisposable data)
            {
                _toDispose.Add(data);
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                if (_legacyDictionary != null)
                {
                    foreach (var kvp in _legacyDictionary)
                    {
                        using (var values = ToCounterGroup(context, kvp.Key, kvp.Value, out var cv))
                        using (var cvLsv = context.GetLazyString(cv))
                        using (var keyLsv = context.GetLazyString(kvp.Key))
                        {
                            PutCounters(context, new CounterGroupDetail
                            {
                                ChangeVector = cvLsv,
                                DocumentId = keyLsv,
                                Values = values
                            });
                        }
                    }

                    UpdateDocumentsMetadata(context);

                    return _legacyDictionary.Count;
                }

                foreach (var cgd in _counterGroups)
                {
                    using (cgd.Values)
                    {
                        PutCounters(context, cgd);
                    }
                }

                UpdateDocumentsMetadata(context);

                return _counterGroups.Count;
            }

            private void UpdateDocumentsMetadata(DocumentsOperationContext context)
            {
                foreach (var toUpdate in _counterUpdates)
                {
                    var doc = toUpdate.Value;
                    using (doc.Data)
                    {
                        UpdateDocumentCountersAfterImportBatch(context, toUpdate.Key, doc);
                    }
                }
            }

            private void PutCounters(DocumentsOperationContext context, CounterGroupDetail counterGroupDetail)
            {
                Document doc;
                string docCollection = null;

                try
                {
                    LoadDocument();
                }
                catch (DocumentDoesNotExistException e)
                {
                    ErrorCount++;
                    _result.AddError(e.Message);
                    return;
                }

                if (doc != null)
                    docCollection = CollectionName.GetCollectionName(doc.Data);

                _database.DocumentsStorage.CountersStorage.PutCounters(context, counterGroupDetail.DocumentId, docCollection,
                    counterGroupDetail.ChangeVector, counterGroupDetail.Values);

                context.LastDatabaseChangeVector = ChangeVectorUtils.MergeVectors(counterGroupDetail.ChangeVector, context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context));

                if (doc?.Data != null &&
                    _counterUpdates.ContainsKey(counterGroupDetail.DocumentId) == false)
                {
                    _counterUpdates.Add(counterGroupDetail.DocumentId, doc);
                }

                void LoadDocument()
                {
                    if (_counterUpdates.TryGetValue(counterGroupDetail.DocumentId, out doc))
                        return;

                    try
                    {
                        doc = _database.DocumentsStorage.Get(context, counterGroupDetail.DocumentId,
                            throwOnConflict: true);

                        if (doc == null)
                        {
                            ThrowMissingDocument(counterGroupDetail.DocumentId);
                            return; // never hit
                        }

                        if (doc.Flags.HasFlag(DocumentFlags.Artificial))
                            ExecuteCounterBatchCommand.ThrowArtificialDocument(doc);

                        docCollection = CollectionName.GetCollectionName(doc.Data);
                    }
                    catch (DocumentConflictException)
                    {
                        // this is fine, we explicitly support
                        // setting the flag if we are in conflicted state is
                        // done by the conflict resolver

                        // avoid loading same document again, we validate write using the metadata instance
                        doc = new Document();
                        docCollection = _database.DocumentsStorage.ConflictsStorage.GetCollection(context, counterGroupDetail.DocumentId);
                    }
                }
            }

            private void UpdateDocumentCountersAfterImportBatch(DocumentsOperationContext context, string docId, Document doc)
            {
                var data = doc.Data;
                data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);

                var flags = doc.Flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.Resolved);
                flags |= DocumentFlags.HasCounters;

                var counterNames = context.DocumentDatabase.DocumentsStorage.CountersStorage.GetCountersForDocument(context, docId);

                data.Modifications = new DynamicJsonValue(data);
                if (metadata == null)
                {
                    data.Modifications[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Counters] = counterNames
                    };
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata)
                    {
                        [Constants.Documents.Metadata.Counters] = counterNames
                    };

                    data.Modifications[Constants.Documents.Metadata.Key] = metadata;
                }

                using (data)
                {
                    var newDocumentData = context.ReadObject(doc.Data, doc.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                    _database.DocumentsStorage.Put(context, doc.Id, null, newDocumentData, flags: flags,
                        nonPersistentFlags: NonPersistentDocumentFlags.ByCountersUpdate | NonPersistentDocumentFlags.FromSmuggler);
                }
            }

            private static unsafe BlittableJsonReaderObject ToCounterGroup(DocumentsOperationContext context, string docId, Dictionary<string, List<(string ChangeVector, long Value)>> dict, out string lastCv)
            {
                lastCv = null;
                var dbIds = new Dictionary<string, int>();
                var counters = new DynamicJsonValue();
                var counterModificationScopes = new List<ByteStringContext<ByteStringMemoryCache>.InternalScope>();

                try
                {
                    foreach (var kvp in dict)
                    {
                        var sizeToAllocate = CountersStorage.SizeOfCounterValues * (kvp.Value.Count + dbIds.Count);

                        counterModificationScopes.Add(context.Allocator.Allocate(sizeToAllocate, out var newVal));

                        var name = kvp.Key;
                        foreach (var tuple in kvp.Value)
                        {
                            var dbId = tuple.ChangeVector.Substring(tuple.ChangeVector.Length - CountersStorage.DbIdAsBase64Size);
                            if (dbIds.TryGetValue(dbId, out var dbIdIndex) == false)
                            {
                                dbIdIndex = dbIds.Count;
                                dbIds.TryAdd(dbId, dbIdIndex);
                            }

                            var etag = ChangeVectorUtils.GetEtagById(tuple.ChangeVector, dbId);

                            var newEntry = (CountersStorage.CounterValues*)newVal.Ptr + dbIdIndex;
                            newEntry->Value = tuple.Value;
                            newEntry->Etag = etag;
                        }

                        lastCv = kvp.Value[kvp.Value.Count - 1].ChangeVector;

                        counters[name] = new BlittableJsonReaderObject.RawBlob(newVal.Ptr, CountersStorage.SizeOfCounterValues * kvp.Value.Count);
                    }

                    var values = context.ReadObject(new DynamicJsonValue
                    {
                        [CountersStorage.DbIds] = dbIds.Keys,
                        [CountersStorage.Values] = counters
                    }, docId);

                    return values;
                }
                finally
                {
                    foreach (var scope in counterModificationScopes)
                    {
                        scope.Dispose();
                    }
                }
            }

            public void Dispose()
            {
                if (_isDisposed)
                    return;

                _isDisposed = true;

                _counterGroups.Clear();

                _counterUpdates.Clear();

                foreach (var disposable in _toDispose)
                {
                    disposable.Dispose();
                }
                _toDispose.Clear();

                _legacyDictionary?.Clear();

                _resetContext?.Dispose();
                _resetContext = null;

                _result.Counters.ErroredCount += ErrorCount;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new SmugglerCounterBatchCommandDto
                {
                    CounterGroups = _counterGroups,
                    LegacyDictionary = _legacyDictionary
                };
            }
        }

        [RavenAction("/databases/*/counters", "GET", AuthorizationStatus.ValidUser)]
        public async Task Get()
        {
            var docId = GetStringValuesQueryString("docId");
            var full = GetBoolValueQueryString("full", required: false) ?? false;
            var counters = GetStringValuesQueryString("counter", required: false);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                CountersDetail countersDetail;
                using (context.OpenReadTransaction())
                {
                    countersDetail = GetInternal(Database, context, counters, docId, full);
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, countersDetail.ToJson());
                }
            }
        }

        public static CountersDetail GetInternal(DocumentDatabase database, DocumentsOperationContext context, Microsoft.Extensions.Primitives.StringValues counters, string docId, bool full)
        {
            var result = new CountersDetail();
            var names = counters.Count != 0
                ? counters
                : database.DocumentsStorage.CountersStorage.GetCountersForDocument(context, docId);

            foreach (var counter in names)
            {
                GetCounterValue(context, database, docId, counter, full, result);
            }

            return result;
        }

        [RavenAction("/databases/*/counters", "POST", AuthorizationStatus.ValidUser)]
        public async Task Batch()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var countersBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "counters");

                var counterBatch = JsonDeserializationClient.CounterBatch(countersBlittable);

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(countersBlittable.ToString(), TrafficWatchChangeType.Counters);
                var cmd = new ExecuteCounterBatchCommand(Database, counterBatch);

                if (cmd.HasWrites)
                {
                    try
                    {
                        await Database.TxMerger.Enqueue(cmd);
                    }
                    catch (DocumentDoesNotExistException)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        throw;
                    }
                }
                else
                {
                    using (context.OpenReadTransaction())
                    {
                        cmd.ExecuteDirectly(context);
                    }
                }
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, cmd.CountersDetail.ToJson());
                }
            }
        }

        private static void GetCounterValue(DocumentsOperationContext context, DocumentDatabase database, string docId,
            string counterName, bool addFullValues, CountersDetail result, bool capValueOnOverflow = false)
        {
            long value = 0;
            long etag = 0;
            result.Counters ??= new List<CounterDetail>();
            Dictionary<string, long> fullValues = null;

            if (addFullValues)
            {
                fullValues = new Dictionary<string, long>();
                foreach (var partialValue in database.DocumentsStorage.CountersStorage.GetCounterPartialValues(context, docId, counterName))
                {
                    etag = HashCode.Combine(etag, partialValue.Etag);
                    try
                    {
                        value = checked(value + partialValue.PartialValue);
                    }
                    catch (OverflowException e)
                    {
                        if (capValueOnOverflow == false)
                            CounterOverflowException.ThrowFor(docId, counterName, e);

                        value = value + partialValue.PartialValue > 0 ?
                            long.MinValue :
                            long.MaxValue;
                    }

                    fullValues[partialValue.ChangeVector] = partialValue.PartialValue;
                }

                if (fullValues.Count == 0)
                {
                    result.Counters.Add(null);
                    return;
                }
            }
            else
            {
                var v = database.DocumentsStorage.CountersStorage.GetCounterValue(context, docId, counterName, capValueOnOverflow);

                if (v == null)
                {
                    result.Counters.Add(null);
                    return;
                }

                value = v.Value.Value;
                etag = v.Value.Etag;
            }

            result.Counters.Add(new CounterDetail
            {
                DocumentId = docId,
                CounterName = counterName,
                TotalValue = value,
                CounterValues = fullValues,
                Etag = etag
            });
        }

        private static void ThrowMissingDocument(string docId)
        {
            throw new DocumentDoesNotExistException(docId, "Cannot operate on counters of a missing document.");
        }
    }

    public class ExecuteCounterBatchCommandDto : TransactionOperationsMerger.IReplayableCommandDto<CountersHandler.ExecuteCounterBatchCommand>
    {
        public bool ReplyWithAllNodesValues;
        public bool FromEtl;
        public List<CounterOperation> List;

        public CountersHandler.ExecuteCounterBatchCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new CountersHandler.
                ExecuteCounterBatchCommand(database, List, ReplyWithAllNodesValues, FromEtl);
            return command;
        }
    }

    public class SmugglerCounterBatchCommandDto : TransactionOperationsMerger.IReplayableCommandDto<CountersHandler.SmugglerCounterBatchCommand>
    {
        public List<CounterGroupDetail> CounterGroups;
        public Dictionary<string, Dictionary<string, List<(string ChangeVector, long Value)>>> LegacyDictionary;

        public CountersHandler.SmugglerCounterBatchCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new CountersHandler.
                SmugglerCounterBatchCommand(database, CounterGroups, LegacyDictionary);
            return command;
        }
    }
}
