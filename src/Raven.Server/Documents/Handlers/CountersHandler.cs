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
using Sparrow.Json;

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
            private readonly bool _fromSmuggler;
            private readonly Dictionary<string, List<CounterOperation>> _dictionary;
            public long ErrorCount;
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
                        Add(docOps.DocumentId, operation, out _);
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

            // used only from smuggler import
            public ExecuteCounterBatchCommand(DocumentDatabase database)
            {
                _fromSmuggler = true;
                _database = database;
                _dictionary = new Dictionary<string, List<CounterOperation>>();
                ErrorCount = 0;
            }

            public int Add(string id, CounterOperation op, out bool isNew)
            {
                isNew = false;
                if (_dictionary.TryGetValue(id, out var existing) == false)
                {
                    isNew = true;
                    _dictionary[id] = new List<CounterOperation> { op };
                    return 1;
                }
                existing.Add(op);
                return existing.Count;
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                var ops = 0;
                var countersToAdd = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                var countersToRemove = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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
                                try
                                {
                                    LoadDocument();
                                }
                                catch (DocumentDoesNotExistException)
                                {
                                    if (_fromSmuggler)
                                    {
                                        ErrorCount++;
                                        continue;
                                    }
                                    throw;
                                }

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
                                if (_fromEtl && doc == null)
                                    break;

                                // intentionally not setting LastChangeVector, we never use it for
                                // etl / import and it isn't meaningful in those scenarios

                                if (_fromEtl)
                                {
                                    _database.DocumentsStorage.CountersStorage.PutCounter(context, docId, docCollection,
                                        operation.CounterName, operation.Delta);
                                }
                                else
                                {
                                    _database.DocumentsStorage.CountersStorage.PutCounter(context, docId, docCollection,
                                        operation.CounterName, operation.ChangeVector, operation.Delta);
                                }

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

            private static void ThrowArtificialDocument(Document doc)
            {
                throw new InvalidOperationException($"Document '{doc.Id}' has '{nameof(DocumentFlags.Artificial)}' flag set. " +
                                                    "Cannot put Counters on artificial documents.");
            }

            private static void ThrowInvalidBatchOperationType(CounterOperation operation)
            {
                throw new ArgumentOutOfRangeException($"Unknown value {operation.Type}");
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

        private static void ThrowInvalidDocumentWithNoMetadata(Document doc)
        {
            throw new InvalidOperationException("Cannot increment counters for " + doc + " because the document has no metadata. Should not happen ever");
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
}
