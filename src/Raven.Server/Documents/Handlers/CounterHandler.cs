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
        [RavenAction("/databases/*/counters/increment", "PUT", AuthorizationStatus.ValidUser)]
        public Task Increment()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var value = GetLongQueryString("val", false) ?? 0;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenWriteTransaction())
            {
                Database.DocumentsStorage.CountersStorage.IncrementCounter(context, id, name, value);
                context.Transaction.Commit();
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/counters/getValue", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCounterValue()
        {
            var documentId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var value = Database.DocumentsStorage.CountersStorage.GetCounterValue(context, documentId, name);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Value"] = value
                    });
                }
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            return Task.CompletedTask;
        }


        [RavenAction("/databases/*/counters/getValues", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCounterValues()
        {
            var documentId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var dic = Database.DocumentsStorage.CountersStorage.GetCounterValues(context, documentId, name);
                var jsonMap = new DynamicJsonValue();
                foreach (var kvp in dic)
                {
                    jsonMap[kvp.Key.ToString()] = kvp.Value;
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Values"] = jsonMap
                    });
                }
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/counters/getNames", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCountersForDocument()
        {
            var documentId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var skip = GetIntValueQueryString("skip", false) ?? 0;
            var take = GetIntValueQueryString("skip", false) ?? 1024;

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
    }
}
