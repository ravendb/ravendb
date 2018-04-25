// -----------------------------------------------------------------------
//  <copyright file="CounterHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class CounterHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/counters/increment", "PUT", AuthorizationStatus.ValidUser)]
        public Task Increment()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
                var value = GetLongQueryString("val", false) ?? 0;

                using (context.OpenWriteTransaction())
                {
                    Database.DocumentsStorage.CountersStorage.IncrementCounter(context, id, name, value);
                    context.Transaction.Commit();
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            }

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
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Value"] = value
                    });
                }
            }

            return Task.CompletedTask;
        }

        /*

                [RavenAction("/databases/#1#attachments", "DELETE", AuthorizationStatus.ValidUser)]
                public async Task Delete()
                {
                    using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                        var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

                        var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                        var cmd = new MergedDeleteAttachmentCommand
                        {
                            Database = Database,
                            ExpectedChangeVector = changeVector,
                            DocumentId = id,
                            Name = name
                        };
                        await Database.TxMerger.Enqueue(cmd);
                        cmd.ExceptionDispatchInfo?.Throw();

                        NoContentStatus();
                    }
                }*/

    }
}
