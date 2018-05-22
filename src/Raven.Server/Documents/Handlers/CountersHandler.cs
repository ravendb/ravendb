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
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Json.Converters;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class CountersHandler : DatabaseRequestHandler
    {
        public class ExecuteCounterBatchCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public bool HasWrites;
            public CountersDetail CountersDetail;
            public string LastChangeVector;

            private readonly DocumentDatabase _database;
            private readonly bool _replyWithAllNodesValues;
            private readonly Dictionary<string, List<CounterOperation>> _dictionary;

            public ExecuteCounterBatchCommand(DocumentDatabase database, CounterBatch counterBatch)
            {
                _database = database;
                _dictionary = new Dictionary<string, List<CounterOperation>>();
                _replyWithAllNodesValues = counterBatch?.ReplyWithAllNodesValues?? false;
                CountersDetail = new CountersDetail
                {
                    Counters = new List<CounterDetail>()
                };

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

            // used only from smuggler import
            public ExecuteCounterBatchCommand(DocumentDatabase database)
            {
                _database = database;
                _dictionary = new Dictionary<string, List<CounterOperation>>();

                CountersDetail = new CountersDetail
                {
                    Counters = new List<CounterDetail>()
                };
            }

            public void Add(string id, CounterOperation op)
            {
                if (_dictionary.TryGetValue(id, out var existing) == false)
                {
                    _dictionary[id] = new List<CounterOperation> { op };
                }

                else
                {
                    existing.Add(op);
                }
            }

            public override int Execute(DocumentsOperationContext context)
            {
                foreach (var kvp in _dictionary)
                {
                    Document doc = null;
                    BlittableJsonReaderObject metadata = null;
                    
                    foreach (var operation in kvp.Value)
                    {
                        switch (operation.Type)
                        {
                            case CounterOperationType.Increment:
                                LoadDocument();
                                LastChangeVector = _database.DocumentsStorage.CountersStorage.IncrementCounter(context, kvp.Key,
                                    operation.CounterName, operation.Delta);
                                GetCounterValue(context, _database, kvp.Key, operation.CounterName, _replyWithAllNodesValues, CountersDetail);
                               
                                break;
                            case CounterOperationType.Delete:
                                LoadDocument();
                                LastChangeVector = _database.DocumentsStorage.CountersStorage.DeleteCounter(context, kvp.Key,
                                    operation.CounterName);
                                break;
                            case CounterOperationType.Put:
                                LoadDocument();
                                 _database.DocumentsStorage.CountersStorage.PutCounterFromReplication(context, kvp.Key,
                                    operation.CounterName, operation.ChangeVector, operation.Delta);
                                LastChangeVector = operation.ChangeVector;
                                break;
                            case CounterOperationType.None:
                                break;
                            case CounterOperationType.Get:
                                GetCounterValue(context, _database, kvp.Key, operation.CounterName, _replyWithAllNodesValues, CountersDetail);
                                break;
                            default:
                                ThrowInvalidBatchOperationType(operation);
                                break;
                        }
                    }

                    if (metadata != null)
                    {
                        UpdateDocumentCounters(metadata, kvp.Value);

                        if (metadata.Modifications != null)
                        {
                            var data = context.ReadObject(doc.Data, kvp.Key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                            var flags = data.TryGet(Constants.Documents.Metadata.Key, out metadata) && 
                                        metadata.TryGet(Constants.Documents.Metadata.Counters, out object _)
                                        ? DocumentFlags.HasCounters
                                        : DocumentFlags.None;

                            _database.DocumentsStorage.Put(context, kvp.Key, null, data, flags: flags); 
                        }
                    }

                    void LoadDocument()
                    {
                        if (doc != null)
                            return;
                        try
                        {
                            doc = _database.DocumentsStorage.Get(context, kvp.Key,
                                throwOnConflict: true);
                            if (doc == null)
                            {
                                ThrowMissingDocument(kvp.Key);
                                return; // never hit
                            }

                            if (doc.TryGetMetadata(out metadata) == false)
                                ThrowInvalidDocumentWithNoMetadata(doc);
                        }
                        catch (DocumentConflictException)
                        {
                            // this is fine, we explicitly support
                            // setting the flag if we are in conflicted state is 
                            // done by the conflict resolver

                            // avoid loading same document again, we validate write using the metadata instance
                            doc = new Document();
                        }
                    }
                }

                return CountersDetail.Counters.Count;
            }

            private static void ThrowInvalidBatchOperationType(CounterOperation operation)
            {
                throw new ArgumentOutOfRangeException($"Unknown value {operation.Type}");
            }

            private void UpdateDocumentCounters(BlittableJsonReaderObject metadata, List<CounterOperation> countersOperations)
            {
                List<string> updates = null;
                if (metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters))
                {
                    foreach (var operation in countersOperations)
                    {
                        // we need to check the updates to avoid inserting duplicate counter names
                        int loc = updates?.BinarySearch(operation.CounterName, StringComparer.OrdinalIgnoreCase) ??
                                  counters.BinarySearch(operation.CounterName, StringComparison.OrdinalIgnoreCase);

                        switch (operation.Type)
                        {
                            case CounterOperationType.Increment:
                            case CounterOperationType.Put:
                                if (loc < 0)
                                {
                                    CreateUpdatesIfNeeded();
                                    updates.Insert(~loc, operation.CounterName);
                                }

                                break;
                            case CounterOperationType.Delete:
                                if (loc >= 0)
                                {
                                    CreateUpdatesIfNeeded();
                                    updates.RemoveAt(loc);
                                }
                                break;
                            case CounterOperationType.None:
                            case CounterOperationType.Get:
                                break;
                            default:
                                ThrowInvalidBatchOperationType(operation);
                                break;
                        }
                    }
                }
                else
                {
                    updates = new List<string>(countersOperations.Count);
                    foreach (var operation in countersOperations)
                    {
                        updates.Add(operation.CounterName);
                    }
                    updates.Sort(StringComparer.OrdinalIgnoreCase);
                }

                if (updates != null)
                {
                    if (updates.Count == 0)
                    {
                        metadata.Modifications = new DynamicJsonValue(metadata);
                        metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);
                    }
                    else
                    {
                        metadata.Modifications = new DynamicJsonValue(metadata)
                        {
                            [Constants.Documents.Metadata.Counters] = new DynamicJsonArray(updates)
                        };
                    }
                }

                void CreateUpdatesIfNeeded()
                {
                    if (updates != null)
                        return;

                    updates = new List<string>(counters.Length + countersOperations.Count);
                    for (int i = 0; i < counters.Length; i++)
                    {
                        var val = counters.GetStringByIndex(i);
                        if (val == null)
                            continue;
                        updates.Add(val);
                    }
                }
            }

            private static void ThrowMissingDocument(string docId)
            {
                throw new CounterDocumentMissingException($"There is no document '{docId}' (or it has been deleted), cannot operate on counters of a missing document");
            }
        }

        [RavenAction("/databases/*/counters", "GET", AuthorizationStatus.ValidUser)]
        public Task Get()
        {
            var docId = GetStringValuesQueryString("docId"); 
            var full = GetBoolValueQueryString("full", required: false) ?? false;
            var counters = GetStringValuesQueryString("counter", required: false);
            var countersDetail = new CountersDetail();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var names = counters.Count != 0 ? 
                                counters : 
                                Database.DocumentsStorage.CountersStorage.GetCountersForDocument(context, docId);
                    foreach (var counter in names)
                    {
                        GetCounterValue(context, Database, docId, counter, full, countersDetail);
                    }
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, countersDetail.ToJson());
                    writer.Flush();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/counters", "POST", AuthorizationStatus.ValidUser)]
        public async Task Batch()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var countersBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "counters");

                var counterBatch = JsonDeserializationClient.CounterBatch(countersBlittable);

                var cmd = new ExecuteCounterBatchCommand(Database, counterBatch);

                if (cmd.HasWrites)
                {
                    try
                    {
                        await Database.TxMerger.Enqueue(cmd);
                    }
                    catch (CounterDocumentMissingException)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        throw;
                    }
                }
                else
                {
                    using (context.OpenReadTransaction())
                    {
                        cmd.Execute(context);
                    }
                }
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, cmd.CountersDetail.ToJson());
                    writer.Flush();
                }
            }
        }

        private static void GetCounterValue(DocumentsOperationContext context, DocumentDatabase database, string docId, string counterName, bool addFullValues, CountersDetail result)
        {
            var fullValues = addFullValues ? new Dictionary<string, long>() : null;
            long? value = null;
            foreach (var (cv, val) in database.DocumentsStorage.CountersStorage.GetCounterValues(context,
                docId, counterName))
            {
                value = value ?? 0;
                value += val;

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
    }
}
