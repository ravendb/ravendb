// -----------------------------------------------------------------------
//  <copyright file="CounterHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
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
        public class IncrementCounterCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;
            private readonly string _doc, _counter;
            private readonly long _value;
            private readonly bool _failTx;

            private CounterBatch _counterBatch;

            public CountersDetail CountersDetail;

            public long CurrentValue;
            public bool DocumentMissing;

            public IncrementCounterCommand(DocumentDatabase database, CounterBatch counterBatch, bool failTx)
            {
                _database = database;
                _counterBatch = counterBatch;
                _failTx = failTx;

                CountersDetail = new CountersDetail();
            }

            public override int Execute(DocumentsOperationContext context)
            {
                foreach (var counter in _counterBatch.Counters)
                {
                    _database.DocumentsStorage.CountersStorage.IncrementCounter(context, counter.DocumentId, counter.CounterName, counter.Delta);

                    try
                    {
                        var doc = _database.DocumentsStorage.Get(context, counter.DocumentId,
                            throwOnConflict: true);
                        if (doc == null)
                        {
                            DocumentMissing = true;
                            if (_failTx == false)
                                return 0;
                            ThrowMissingDocument();
                            return 0; // never hit
                        }

                        if (doc.TryGetMetadata(out var metadata) == false)
                            ThrowInvalidDocumentWithNoMetadata(doc, _counter);

                        UpdateDocumentCounters(metadata);

                        if (metadata.Modifications != null)
                        {
                            var data = context.ReadObject(doc.Data, counter.DocumentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                            _database.DocumentsStorage.Put(context, counter.DocumentId, null, data, flags: DocumentFlags.HasCounters);
                        }
                    }
                    catch (DocumentConflictException)
                    {
                        // this is fine, we explicitly support
                        // setting the flag if we are in conflicted state is 
                        // done by the conflict resolver
                    }

                    var fullValues = new Dictionary<string, long>();
                    long value = 0;
                    foreach (var (cv, val) in _database.DocumentsStorage.CountersStorage.GetCounterValues(context, counter.DocumentId, counter.CounterName))
                    {
                        value += val;
                        if (fullValues != null)
                        {
                            fullValues[cv] = val;
                        }
                    }

                    CountersDetail.Counters.Add(new CounterDetail
                    {
                        DocumentId = counter.DocumentId,
                        CounterName = counter.CounterName,
                        TotalValue = value,
                        CounterValues = fullValues
                    });
                }

                return 1;
            }

            private void UpdateDocumentCounters(BlittableJsonReaderObject metadata)
            {
                if (metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters) == false)
                {
                    metadata.Modifications = new DynamicJsonValue(metadata)
                    {
                        [Constants.Documents.Metadata.Counters] = new DynamicJsonArray
                        {
                            _counter
                        }
                    };
                }
                else
                {
                    var loc = counters.BinarySearch(_counter);
                    if (loc < 0)
                    {
                        loc = ~loc; // flip the bits and find the right location
                        var list = new List<string>(counters.Length + 1);
                        for (int i = 0; i < loc; i++)
                        {
                            list.Add(counters.GetStringByIndex(i));
                        }

                        list.Add(_counter);
                        for (int i = loc; i < counters.Length; i++)
                        {
                            list.Add(counters.GetStringByIndex(i));
                        }

                        metadata.Modifications = new DynamicJsonValue(metadata)
                        {
                            [Constants.Documents.Metadata.Counters] = new DynamicJsonArray(list)
                        };
                    }
                }
            }

            public void ThrowMissingDocument()
            {
                throw new CounterDocumentMissingException($"There is no document '{_doc}' (or it has been deleted), cannot set counter '{_counter}' for a missing document");
            }
        }

        public class DeleteCounterCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;
            private readonly GetOrDeleteCounters _countersBatch;

            public DeleteCounterCommand(DocumentDatabase database, GetOrDeleteCounters counters)
            {
                _database = database;
                _countersBatch = counters;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                foreach (var counterOperation in _countersBatch.Counters)
                {
                    foreach (var counterName in counterOperation.Counters)
                    {
                        var id = counterOperation.DocumentId;
                        var del = _database.DocumentsStorage.CountersStorage.DeleteCounter(context, id, counterName);

                        if (del == false)
                            return 0;
                        try
                        {
                            var doc = _database.DocumentsStorage.Get(context, id);
                            if (doc == null)
                                return 0;
                            if (doc.TryGetMetadata(out var metadata) == false)
                                ThrowInvalidDocumentWithNoMetadata(doc, counterName);

                            UpdateDocumentCounters(metadata, counterName);
                            if (metadata.Modifications != null)
                            {
                                var data = context.ReadObject(doc.Data, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                                _database.DocumentsStorage.Put(context, id, null, data);
                            }
                        }
                        catch (DocumentConflictException)
                        {
                            // this is fine, we explicitly support
                            // setting the flag if we are in conflicted state is 
                            // done by the conflict resolver
                        }

                    }

                }


                return 1;
            }

            private void UpdateDocumentCounters(BlittableJsonReaderObject metadata, string counter)
            {
                if (metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters))
                {

                    var counterIndex = counters.BinarySearch(counter);
                    if (counterIndex < 0)
                        return;

                    metadata.Modifications = new DynamicJsonValue(metadata);

                    if (counters.Length == 1)
                    {
                        //document has only one counter, can remove @counters property from metadata
                        metadata.Modifications.Removals = new HashSet<int>
                        {
                            metadata.GetPropertyIndex(Constants.Documents.Metadata.Counters)
                        };
                    }
                    else
                    {
                        metadata.Modifications[Constants.Documents.Metadata.Counters] = new DynamicJsonArray(
                            counters.Where((e, index) => index != counterIndex));
                    }
                }
            }
        }

        [RavenAction("/databases/*/counters/batch", "POST", AuthorizationStatus.ValidUser)]
        public async Task Batch()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var countersBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "counters");

                var counterBatch = JsonDeserializationClient.CounterBatch(countersBlittable);

                var cmd = new IncrementCounterCommand(Database, counterBatch, failTx: false);

                await Database.TxMerger.Enqueue(cmd);

                if (cmd.DocumentMissing)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    cmd.ThrowMissingDocument();
                    return; // never hit
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, cmd.CountersDetail.ToJson());
                    writer.Flush();
                }
            }
        }

        [RavenAction("/databases/*/counters", "POST", AuthorizationStatus.ValidUser)]
        public async Task GetCounters()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var full = GetBoolValueQueryString("full", required: false) ?? false;
                BlittableJsonReaderObject countersBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "counters");
                var counters = JsonDeserializationClient.CountersBatch(countersBlittable).Counters;
                var result = new CountersDetail();

                foreach (var getCounterDetails in counters)
                {
                    foreach (var counter in getCounterDetails.Counters)
                    {
                        var id = getCounterDetails.DocumentId;
                        var fullValues = full ? new Dictionary<string, long>() : null;
                        long? value = null;
                        foreach (var (cv, val) in Database.DocumentsStorage.CountersStorage.GetCounterValues(context, id, counter))
                        {
                            value = value ?? 0;
                            value += val;
                            if (fullValues != null)
                            {
                                fullValues[cv] = val;
                            }
                        }

                        if (value == null)
                            continue;

                        result.Counters.Add(new CounterDetail
                        {
                            CounterName = counter,
                            DocumentId = id,
                            CounterValues = fullValues,
                            TotalValue = value.Value
                        });
                    }
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result.ToJson());
                    writer.Flush();
                }
            }
        }

        [RavenAction("/databases/*/counters/delete", "POST", AuthorizationStatus.ValidUser)]
        public async Task Delete()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var countersBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "counters");
                var countersToDelete = JsonDeserializationClient.CountersBatch(countersBlittable);
                var cmd = new DeleteCounterCommand(Database, countersToDelete);
                await Database.TxMerger.Enqueue(cmd);

                NoContentStatus();
            }
        }

        private static void ThrowInvalidDocumentWithNoMetadata(Document doc, string counter)
        {
            throw new InvalidOperationException("Cannot increment counter " + counter + " for " + doc + " because the document has no metadata. Should not happen ever");
        }
    }
}
