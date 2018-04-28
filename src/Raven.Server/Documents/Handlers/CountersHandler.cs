// -----------------------------------------------------------------------
//  <copyright file="CounterHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class CountersHandler : DatabaseRequestHandler
    {
        public class IncrementCounterCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private DocumentDatabase _database;
            private string _doc, _counter;
            private long _value;
            private bool _failTx;

            public long CurrentValue;
            public bool DocumentMissing;

            public IncrementCounterCommand(DocumentDatabase database, string doc, string counter, long value, bool failTx)
            {
                _database = database;
                _doc = doc;
                _counter = counter;
                _value = value;
                _failTx = failTx;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                try
                {
                    var doc = _database.DocumentsStorage.Get(context, _doc,
                        throwOnConflict: true);
                    if(doc == null)
                    {
                        DocumentMissing = true;
                        if (_failTx == false)
                            return 0;
                        ThrowMissingDocument();
                    }

                }
                catch (DocumentConflictException)
                {
                    // this is fine, we explicitly support
                    // setting the flag if we are in conflicted state is 
                    // done by the conflict resolver
                }

                _database.DocumentsStorage.CountersStorage.IncrementCounter(context, _doc, _counter, _value);
                CurrentValue = _database.DocumentsStorage.CountersStorage.GetCounterValue(context, _doc, _counter) ?? 0;
                return 1;
            }

            public void ThrowMissingDocument()
            {
                throw new CounterDocumentMissingException($"There is no document '{_doc}' (or it has been deleted), cannot set counter '{_counter}' for a missing document");
            }
        }

        public class DeleteCounterCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private DocumentDatabase _database;
            private string _doc, _counter;

            public DeleteCounterCommand(DocumentDatabase database, string doc, string counter)
            {
                _database = database;
                _doc = doc;
                _counter = counter;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                var del = _database.DocumentsStorage.CountersStorage.DeleteCounter(context, _doc, _counter);
                return del ? 1 : 0;
            }
        }

        [RavenAction("/databases/*/counters", "POST", AuthorizationStatus.ValidUser)]
        public async Task Increment()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("doc");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var value = GetLongQueryString("val", true) ?? 1;

            var cmd = new IncrementCounterCommand(Database, id, name, value, failTx: false);

            await Database.TxMerger.Enqueue(cmd);

            if (cmd.DocumentMissing)
            {
                
                using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    cmd.ThrowMissingDocument();
                }
                return;// never hit
            }

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Document");
                writer.WriteString(id);
                writer.WriteComma();
                writer.WritePropertyName("Counter");
                writer.WriteString(name);
                writer.WriteComma();
                writer.WritePropertyName("Value");
                writer.WriteInteger(cmd.CurrentValue);
                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/counters", "GET", AuthorizationStatus.ValidUser)]
        public Task Get()
        {
            var documentId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("doc");
            var name = GetStringQueryString("name", required: false);

            if (string.IsNullOrEmpty(name))
                return GetCountersForDocument(documentId);

            return GetCounterValue(documentId, name);
        }

        private Task GetCounterValue(string documentId, string name)
        {
            var mode = GetStringQueryString("mode", required: false);

            switch (mode)
            {
                default: // likely to be the common option
                    return GetSingleCounterValue(documentId, name);

                case "all":
                case "ALL":
                case "All":
                    return GetCounterValues(documentId, name);
            }

        }

        private Task GetSingleCounterValue(string documentId, string name)
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var value = Database.DocumentsStorage.CountersStorage.GetCounterValue(context, documentId, name);
                if (value == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return Task.CompletedTask;
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Document");
                    writer.WriteString(documentId);
                    writer.WriteComma();
                    writer.WritePropertyName("Counter");
                    writer.WriteString(name);
                    writer.WriteComma();
                    writer.WritePropertyName("Value");
                    writer.WriteInteger(value.Value);
                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        private Task GetCounterValues(string documentId, string name)
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var values = Database.DocumentsStorage.CountersStorage.GetCounterValues(context, documentId, name);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Document");
                    writer.WriteString(documentId);
                    writer.WriteComma();
                    writer.WritePropertyName("Counter");
                    writer.WriteString(name);
                    writer.WriteComma();
                    writer.WritePropertyName("Values");

                    writer.WriteStartArray();
                    var first = true;

                    foreach (var (cv, val) in values)
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;
                        writer.WriteStartObject();
                        writer.WritePropertyName("ChangeVector");
                        writer.WriteString(cv);
                        writer.WriteComma();
                        writer.WritePropertyName("Value");
                        writer.WriteInteger(val);
                        writer.WriteEndObject();
                    }
                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        private Task GetCountersForDocument(string documentId)
        {
            var skip = GetStart();
            var take = GetPageSize();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var counters = Database.DocumentsStorage.CountersStorage.GetCountersForDocument(context, documentId, skip, take);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Document");
                    writer.WriteString(documentId);
                    writer.WriteComma();
                    writer.WriteArray("Counters", counters);
                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/counters", "DELETE", AuthorizationStatus.ValidUser)]
        public async Task Delete()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("doc");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var cmd = new DeleteCounterCommand(Database, id, name);
            await Database.TxMerger.Enqueue(cmd);

            NoContentStatus();
        }
    }
}
