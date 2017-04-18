using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class SubscriptionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/subscriptions", "PUT", "/databases/{databaseName:string}/subscriptions?startEtag={startEtag:long|optional}")]
        public async Task Create()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var startEtag = GetLongQueryString("startEtag", required: false) ?? 0;

                var json = await context.ReadForDiskAsync(RequestBodyStream(), null);
                var subscriptionId = Database.SubscriptionStorage.CreateSubscription(json, startEtag);
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
        public Task Delete()
        {
            var id = GetLongQueryString("id").Value;

            Database.SubscriptionStorage.DeleteSubscription(id);

            return NoContent();
        }

        [RavenAction("/databases/*/subscriptions", "GET", "/databases/{databaseName:string}/subscriptions/running")]
        public Task GetAll()
        {
            var start = GetStart();
            var pageSize = GetPageSize();
            var history = GetBoolValueQueryString("history", required: false) ?? false;
            var running = GetBoolValueQueryString("running", required: false) ?? false;
            var id = GetLongQueryString("id", required: false);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                IEnumerable<DynamicJsonValue> subscriptions;
                if (id.HasValue == false)
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
                            .GetRunningSubscription(context, id.Value, history)
                        : Database
                            .SubscriptionStorage
                            .GetSubscription(context, id.Value, history);

                    if (subscription == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    }

                    subscriptions = new[] { subscription };
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WriteResults(context, subscriptions, (w, c, subscription) =>
                    {
                        c.Write(w, subscription);
                    });

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        // TODO: do we need this?
        [RavenAction("/databases/*/subscriptions/count", "GET", "/databases/{databaseName:string}/subscriptions/running/count")]
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
            var subscriptionId = GetLongQueryString("id").Value;

            if (Database.SubscriptionStorage.DropSubscriptionConnection(subscriptionId, "Dropped by api request") == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            return NoContent();
        }
    }
}
