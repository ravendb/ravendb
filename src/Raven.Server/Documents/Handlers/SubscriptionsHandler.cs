using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Json;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Conventions;
using System.Linq;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;

namespace Raven.Server.Documents.Handlers
{
    public class SubscriptionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/subscriptions/try", "POST", "/databases/{databaseName:string}/subscriptions/try")]
        public async Task Try()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var tryout = JsonDeserializationServer.SubscriptionTryout(json);

                SubscriptionPatchDocument patch = null;
                if (string.IsNullOrEmpty(tryout.Script) == false)
                {
                    patch = new SubscriptionPatchDocument(Database,tryout.Script);
                }

                if(tryout.Collection == null)
                    throw new ArgumentException("Collection must be specified");

                var pageSize = GetIntValueQueryString("pageSize", required: true) ?? 1;

                var fetcher = new SubscriptionDocumentsFetcher(Database, pageSize, -0x42, 
                    new IPEndPoint(HttpContext.Connection.RemoteIpAddress, HttpContext.Connection.RemotePort));

                var state = new SubscriptionState
                {
                    ChangeVector = tryout.ChangeVector,
                    Criteria = new SubscriptionCriteria
                    {
                        Collection = tryout.Collection,
                        IsVersioned = tryout.IsVersioned,
                        Script = tryout.Script
                    },
                };
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();

                    using (context.OpenReadTransaction())
                    {
                        var first = true;

                        foreach (var itemDetails in fetcher.GetDataToSend(context, state, patch, 0))
                        {
                            if (first == false)
                                writer.WriteComma();

                            if (itemDetails.Exception == null)
                            {
                                writer.WriteDocument(context, itemDetails.Doc);
                            }
                            else
                            {
                                writer.WriteObject(context.ReadObject(
                                    new DynamicJsonValue
                                    {
                                        ["Id"] = itemDetails.Doc.Id,
                                        ["Etag"] = itemDetails.Doc.Etag,
                                        ["Exception"] = itemDetails.Exception.ToString(),
                                        ["Document"] = itemDetails.Doc.Data
                                    }, "Subscription process exception"
                                ));
                            }

                            first = false;
                        }
                    }

                    writer.WriteEndArray();
                    writer.WriteEndObject();
                }
            }
        }
        
        [RavenAction("/databases/*/subscriptions", "PUT", "/databases/{databaseName:string}/subscriptions")]
        public async Task Create()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var options = JsonDeserializationServer.SubscriptionCreationParams(json);
                var id = GetLongQueryString("id", required: false);
                var disabled = GetBoolValueQueryString("disabled", required: false);
                var subscriptionId = await Database.SubscriptionStorage.PutSubscription(options, id, disabled);
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created; // Created

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Id"] = subscriptionId
                    });
                }
            }
        }

        [RavenAction("/databases/*/subscriptions", "DELETE", "/databases/{databaseName:string}/subscriptions?id={subscriptionId:long}&taskName={taskName:string}")]
        public async Task Delete()
        {
            var subscriptionName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("taskName");

            await Database.SubscriptionStorage.DeleteSubscription(subscriptionName);

            await NoContent();
        }

        [RavenAction("/databases/*/subscriptions", "GET", "/databases/{databaseName:string}/subscriptions?[running=true|history=true|id=<subscription id>|name=<subscription name>]")]
        public Task GetAll()
        {
            var start = GetStart();
            var pageSize = GetPageSize();
            var history = GetBoolValueQueryString("history", required: false) ?? false;
            var running = GetBoolValueQueryString("running", required: false) ?? false;
            var id = GetLongQueryString("id", required: false);
            var name = GetStringQueryString("name", required: false);

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))                
            using (context.OpenReadTransaction())
            {
                IEnumerable<Subscriptions.SubscriptionStorage.SubscriptionGeneralDataAndStats> subscriptions;
                if (id == null)
                {
                    subscriptions = running
                        ? Database.SubscriptionStorage.GetAllRunningSubscriptions(context, history, start, pageSize)
                        : Database.SubscriptionStorage.GetAllSubscriptions(context, history, start, pageSize);
                }
                else
                {
                    var subscription = running
                        ? Database
                            .SubscriptionStorage
                            .GetRunningSubscription(context, id.Value, name, history)
                        : Database
                            .SubscriptionStorage
                            .GetSubscription(context, name, history);

                    if (subscription == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    }

                    subscriptions = new[] { subscription };
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    DocumentConventions documentConventions = DocumentConventions.Default;

                    writer.WriteStartObject();

                    var subscriptionsAsBlittable = subscriptions.Select(x => EntityToBlittable.ConvertEntityToBlittable(x, documentConventions, context));
                    writer.WriteArray(context, "Results", subscriptionsAsBlittable, (w, c, subscription) =>
                    {
                        c.Write(w, subscription);
                    });

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/subscriptions/drop", "POST", "/databases/{databaseName:string}/subscriptions/drop?id={subscriptionId:long}")]
        public Task DropSubscriptionConnection()
        {
            var subscriptionId = GetLongQueryString("id");
            
            if (Database.SubscriptionStorage.DropSubscriptionConnection(subscriptionId, new SubscriptionClosedException("Dropped by API request")) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            return NoContent();
        }
    }
}
