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

namespace Raven.Server.Documents.Handlers
{
    public class SubscriptionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/subscriptions", "PUT", "/databases/{databaseName:string}/subscriptions")]
        public async Task Create()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var options = JsonDeserializationServer.SubscriptionCreationParams(json);
                var subscriptionId = await Database.SubscriptionStorage.CreateSubscription(options);
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

        [RavenAction("/databases/*/subscriptions", "DELETE", "/databases/{databaseName:string}/subscriptions?id={subscriptionId:long}")]
        public async Task Delete()
        {
            var id = GetStringQueryString("id");

            await Database.SubscriptionStorage.DeleteSubscription(id);

            await NoContent();
        }

        [RavenAction("/databases/*/subscriptions", "GET", "/databases/{databaseName:string}/subscriptions?[running=true|history=true|id=<subscription id>]")]
        public Task GetAll()
        {
            var start = GetStart();
            var pageSize = GetPageSize();
            var history = GetBoolValueQueryString("history", required: false) ?? false;
            var running = GetBoolValueQueryString("running", required: false) ?? false;
            var id = GetStringQueryString("id", required: false);

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))                
            using (context.OpenReadTransaction())
            {
                IEnumerable<Subscriptions.SubscriptionStorage.SubscriptionGeneralDataAndStats> subscriptions;
                if (string.IsNullOrEmpty(id))
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
                            .GetRunningSubscription(context, id, history)
                        : Database
                            .SubscriptionStorage
                            .GetSubscription(context, id, history);

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

        // TODO: do we need this?
        [RavenAction("/databases/*/subscriptions/count", "GET", "/databases/{databaseName:string}/subscriptions/count")]
        public Task GetRunningSubscriptionsCount()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var runningCount = Database.SubscriptionStorage.GetRunningCount();
                    var totalCount = Database.SubscriptionStorage.GetAllSubscriptionsCount();

                    context.Write(writer, new DynamicJsonValue()
                    {
                        ["Running"] = runningCount,
                        ["Total"] = totalCount
                    });
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/subscriptions/drop", "POST", "/databases/{databaseName:string}/subscriptions/drop?id={subscriptionId:long}")]
        public Task DropSubscriptionConnection()
        {
            var subscriptionId = GetStringQueryString("id");
            
            if (Database.SubscriptionStorage.DropSubscriptionConnection(subscriptionId, new SubscriptionClosedException("Dropped by API request")) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            return NoContent();
        }
    }
}
