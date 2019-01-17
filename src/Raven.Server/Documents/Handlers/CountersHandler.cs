// -----------------------------------------------------------------------
//  <copyright file="CountersHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Counters;
using Raven.Client.Json.Converters;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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
            private readonly Dictionary<string, List<CounterOperation>> _dictionary;

            public ExecuteCounterBatchCommand(DocumentDatabase database, CounterBatch counterBatch)
            {
                _database = database;
                _dictionary = new Dictionary<string, List<CounterOperation>>();
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
                        Add(docOps.DocumentId, operation);
                    }
                }
            }

            /// <summary>
            /// Used only from replay Tx commands
            /// </summary>
            public ExecuteCounterBatchCommand(
                DocumentDatabase database,
                Dictionary<string, List<CounterOperation>> operationsPreDocument,
                bool replyWithAllNodesValues,
                bool fromEtl)
            {
                _database = database;
                _replyWithAllNodesValues = replyWithAllNodesValues;
                _fromEtl = fromEtl;
                _dictionary = operationsPreDocument;
            }

            private void Add(string id, CounterOperation op)
            {
                if (_dictionary.TryGetValue(id, out var existing) == false)
                {
                    _dictionary[id] = new List<CounterOperation> { op };
                    return;
                }

                existing.Add(op);
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                var countersToAdd = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                var countersToRemove = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                var ops = 0;
                foreach (var kvp in _dictionary)
                {
                    Document doc = null;
                    var docId = kvp.Key;
                    string docCollection = null;
                    ops += kvp.Value.Count;

                    foreach (var operation in kvp.Value)
                    {
                        switch (operation.Type)
                        {
                            case CounterOperationType.Increment:
                            case CounterOperationType.Delete:
                            case CounterOperationType.Put:
                                LoadDocument();
                                break;
                        }

                        switch (operation.Type)
                        {
                            case CounterOperationType.Increment:
                                LastChangeVector =
                                    _database.DocumentsStorage.CountersStorage.IncrementCounter(context, docId, docCollection, operation.CounterName, operation.Delta, out var exists);
                                GetCounterValue(context, _database, docId, operation.CounterName, _replyWithAllNodesValues, CountersDetail);

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
                        var nonPersistentFlags = NonPersistentDocumentFlags.ByCountersUpdate;
                        if (_fromSmuggler)
                            nonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;

                        var changeVector = _database
                            .DocumentsStorage
                            .CountersStorage
                            .UpdateDocumentCounters(context, doc, docId, countersToAdd, countersToRemove, nonPersistentFlags);

                        if (changeVector != null)
                            LastDocumentChangeVector = LastChangeVector = changeVector;

                        doc.Data.Dispose(); // we cloned the data, so we can dispose it.
                    }

                    countersToAdd.Clear();
                    countersToRemove.Clear();

                    void LoadDocument()
                    {
                        if (doc != null)
                            return;
                        try
                        {
                            doc = _database.DocumentsStorage.Get(context, docId,
                                throwOnConflict: true);
                            if (doc == null)
                            {
                                if (_fromEtl)
                                    return;

                                ThrowMissingDocument(docId);
                                return; // never hit
                            }

                            if (doc.Flags.HasFlag(DocumentFlags.Artificial))
                                ThrowArtificialDocument(doc);

                            docCollection = CollectionName.GetCollectionName(doc.Data);
                        }
                        catch (DocumentConflictException)
                        {
                            if (_fromEtl)
                                return;

                            // this is fine, we explicitly support
                            // setting the flag if we are in conflicted state is 
                            // done by the conflict resolver

                            // avoid loading same document again, we validate write using the metadata instance
                            doc = new Document();
                            docCollection = _database.DocumentsStorage.ConflictsStorage.GetCollection(context, docId);
                        }
                    }
                }

                return ops;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new ExecuteCounterBatchCommandDto
                {
                    Dictionary = _dictionary,
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

        public class SmugglerCounterBatchCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;
            private readonly Dictionary<string, CounterGroup> _dictionary;
            private Dictionary<string, Dictionary<string, List<(string ChangeVector, long Value)>>> _legacyDictionary;

            public SmugglerCounterBatchCommand(DocumentDatabase database)
            {
                _database = database;
                _dictionary = new Dictionary<string, CounterGroup>();
            }

            public void Add(string id, CounterGroup cg)
            {
                _dictionary.Add(id, cg);
            }

            public void AddLegacy(string id, CounterDetail counterDetail, out bool isNew)
            {
                isNew = false;
                _legacyDictionary = _legacyDictionary ?? new Dictionary<string, Dictionary<string, List<(string ChangeVector, long Value)>>>(StringComparer.OrdinalIgnoreCase);
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
                    isNew = true;
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

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                var countersToAdd = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                if (_legacyDictionary != null)
                {
                    foreach (var kvp in _legacyDictionary)
                    {
                        var values = ToCounterGroup(context, kvp.Value, out var cv);
                        PutCounters(context, (kvp.Key, new CounterGroup
                        {
                            ChangeVector = cv,
                            Values = values
                        }), countersToAdd);
                    }

                    return _legacyDictionary.Count;
                }

                foreach (var kvp in _dictionary)
                {
                    PutCounters(context, (kvp.Key, kvp.Value), countersToAdd);
                }

                return _dictionary.Count;
            }

            private void PutCounters(DocumentsOperationContext context, (string DocId, CounterGroup CounterGroup) counters, SortedSet<string> countersToAdd)
            {
                Document doc = null;
                string docCollection = null;
                LoadDocument();

                if (doc != null)
                    docCollection = CollectionName.GetCollectionName(doc.Data);

                _database.DocumentsStorage.CountersStorage.PutCounters(context, counters.DocId, docCollection,
                    counters.CounterGroup.ChangeVector, counters.CounterGroup.Values);

                counters.CounterGroup.Values.TryGet(CountersStorage.Values, out BlittableJsonReaderObject values);
                foreach (var counter in values.GetPropertyNames())
                {
                    countersToAdd.Add(counter);
                }

                if (doc?.Data != null)
                {
                    var nonPersistentFlags = NonPersistentDocumentFlags.ByCountersUpdate |
                                             NonPersistentDocumentFlags.FromSmuggler;

                    _database.DocumentsStorage.CountersStorage.UpdateDocumentCounters(context, doc, counters.DocId, countersToAdd, new HashSet<string>(), nonPersistentFlags);
                    doc.Data?.Dispose(); // we cloned the data, so we can dispose it.
                }

                countersToAdd.Clear();

                void LoadDocument()
                {
                    try
                    {
                        doc = _database.DocumentsStorage.Get(context, counters.DocId,
                            throwOnConflict: true);
                        if (doc == null)
                        {
                            ThrowMissingDocument(counters.DocId);
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
                        docCollection = _database.DocumentsStorage.ConflictsStorage.GetCollection(context, counters.DocId);
                    }
                }


            }

            private static unsafe BlittableJsonReaderObject ToCounterGroup(DocumentsOperationContext context, Dictionary<string, List<(string ChangeVector, long Value)>> dict, out string lastCv)
            {
                lastCv = null;
                var dbIds = new Dictionary<string, int>();
                var counters = new DynamicJsonValue(); 
                var counterModificationScopes = new List<ByteStringContext<ByteStringMemoryCache>.InternalScope>();

                try
                {
                    foreach (var kvp in dict)
                    {
                        var sizeToAllocate = CountersStorage.SizeOfCounterValues * kvp.Value.Count;

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

                        counters[name] = new BlittableJsonReaderObject.RawBlob { Ptr = newVal.Ptr, Length = newVal.Length };
                    }

                    var values = context.ReadObject(new DynamicJsonValue
                    {
                        [CountersStorage.DbIds] = dbIds.Keys,
                        [CountersStorage.Values] = counters
                    }, null);

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

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new SmugglerCounterBatchCommandDto();
            }
        }

        [RavenAction("/databases/*/counters", "GET", AuthorizationStatus.ValidUser)]
        public Task Get()
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

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, countersDetail.ToJson());
                    writer.Flush();
                }
            }

            return Task.CompletedTask;
        }

        public static CountersDetail GetInternal(DocumentDatabase database, DocumentsOperationContext context, StringValues counters, string docId, bool full)
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
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, cmd.CountersDetail.ToJson());
                    writer.Flush();
                }
            }
        }

        private static void GetCounterValue(DocumentsOperationContext context, DocumentDatabase database, string docId,
            string counterName, bool addFullValues, CountersDetail result)
        {
            var fullValues = addFullValues ? new Dictionary<string, long>() : null;
            long? value = null;
            foreach (var (cv, val) in database.DocumentsStorage.CountersStorage.GetCounterValues(context,
                docId, counterName))
            {
                value = value ?? 0;
                try
                {
                    value = checked(value + val);
                }
                catch (OverflowException e)
                {
                    CounterOverflowException.ThrowFor(docId, counterName, e);
                }

                if (addFullValues)
                {
                    fullValues[cv] = val;
                }
            }

            if (value == null)
                return;

            if (result.Counters == null)
                result.Counters = new List<CounterDetail>();

            result.Counters.Add(new CounterDetail
            {
                DocumentId = docId,
                CounterName = counterName,
                TotalValue = value.Value,
                CounterValues = fullValues
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
        public Dictionary<string, List<CounterOperation>> Dictionary;

        public CountersHandler.ExecuteCounterBatchCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new CountersHandler.
                ExecuteCounterBatchCommand(database, Dictionary, ReplyWithAllNodesValues, FromEtl);
            return command;
        }
    }

    public class SmugglerCounterBatchCommandDto : TransactionOperationsMerger.IReplayableCommandDto<CountersHandler.SmugglerCounterBatchCommand>
    {
        public CountersHandler.SmugglerCounterBatchCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new CountersHandler.
                SmugglerCounterBatchCommand(database);
            return command;
        }
    }
}
