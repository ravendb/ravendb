// -----------------------------------------------------------------------
//  <copyright file="CounterHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Net;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class CounterHandler : DatabaseRequestHandler
    {
        public class IncrementCounterCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private DocumentDatabase _database;
            private string _doc, _counter;
            private long _value;

            public long CurrentValue;

            public IncrementCounterCommand(DocumentDatabase database, string doc, string counter, long value)
            {
                _database = database;
                _doc = doc;
                _counter = counter;
                _value = value;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                _database.DocumentsStorage.CountersStorage.IncrementCounter(context, _doc, _counter, _value);
                CurrentValue = _database.DocumentsStorage.CountersStorage.GetCounterValue(context, _doc, _counter) ?? 0;
                return 1;
            }
        }

        public class ResetCounterCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private DocumentDatabase _database;
            private string _doc, _counter;

            public ResetCounterCommand(DocumentDatabase database, string doc, string counter)
            {
                _database = database;
                _doc = doc;
                _counter = counter;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                _database.DocumentsStorage.CountersStorage.ResetCounter(context, _doc, _counter);
                return 1;
            }
        }

        [RavenAction("/databases/*/counters/increment", "PUT", AuthorizationStatus.ValidUser)]
        public async Task Increment()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("doc");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var value = GetLongQueryString("val", true) ?? 1;


            var cmd = new IncrementCounterCommand(Database, id, name, value);

            await Database.TxMerger.Enqueue(cmd);

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

        [RavenAction("/databases/*/counters/getValue", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCounterValue()
        {
            var documentId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("doc");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

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

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/counters/getValues", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCounterValues()
        {
            var documentId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("doc");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var dic = Database.DocumentsStorage.CountersStorage.GetCounterValues(context, documentId, name);
                
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
                    foreach (var (db, val) in dic)
                    {
                        if(first == false)
                            writer.WriteComma();
                        first = false;
                        writer.WriteStartObject();
                        writer.WritePropertyName("DbId");
                        writer.WriteString(db.ToString()); // TODO: use the short referece
                        writer.WriteComma();
                        writer.WritePropertyName("Value");
                        writer.WriteInteger(val);
                        writer.WriteEndObject();
                    }
                    writer.WriteEndArray();
                    writer.WriteEndObject();
                }
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/counters/getNames", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCountersForDocument()
        {
            var documentId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("doc");
            var skip = GetStart();
            var take = GetPageSize();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var counters = Database.DocumentsStorage.CountersStorage.GetCountersForDocument(context, documentId, skip, take);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Names"] = counters
                    });
                }
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/counters/reset", "Delete", AuthorizationStatus.ValidUser)]
        public async Task Reset()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("doc");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var cmd = new ResetCounterCommand(Database, id, name);
            await Database.TxMerger.Enqueue(cmd);

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
        }
    }
}
