using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;
using Sparrow.Json.Parsing;
using Sparrow.Json;
using System.Net.WebSockets;
using System.Diagnostics;
using System.Threading;
using System.IO;
using System.Net.Http.Headers;
using Raven.Abstractions.Util;
using Raven.Server.Extensions;
using Sparrow;
using Sparrow.Binary;


namespace Raven.Server.Documents.Handlers
{
    public class SubscriptionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/subscriptions/create", "POST", "/databases/{databaseName:string}/subscriptions/create?startEtag={startEtag:long|optional}")]
        public async Task Create()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var startEtag = GetLongQueryString("startEtag") ?? 0;

                var subscriptionCriteriaRaw = await context.ReadForDiskAsync(RequestBodyStream(), null).ConfigureAwait(false);
                var subscriptionId = Database.SubscriptionStorage.CreateSubscription(subscriptionCriteriaRaw, startEtag);
                var ack = new DynamicJsonValue
                {
                    ["Id"] = subscriptionId
                };
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, ack);
                }

                HttpContext.Response.StatusCode = 201; // NoContent
            }
        }

        [RavenAction("/databases/*/subscriptions", "DELETE",
            "/databases/{databaseName:string}/subscriptions?id={subscriptionId:long}")]
        public Task Delete()
        {
            var ids = HttpContext.Request.Query["id"];
            if (ids.Count == 0)
                throw new ArgumentException("The 'id' query string parameter is mandatory");

            long id;
            if (long.TryParse(ids[0], out id) == false)
                throw new ArgumentException("The 'id' query string parameter must be a valid long");

            Database.SubscriptionStorage.DeleteSubscription(id);

            HttpContext.Response.StatusCode = 204; // NoContent

            return Task.CompletedTask;
        }
        

        [RavenAction("/databases/*/subscriptions/pull", "GET",
            "/databases/{databaseName:string}/subscriptions/pull?id={subscriptionId:long}&connection={connection:string}&strategy={strategy:string}&maxDocsPerBatch={maxDocsPerBatch:int}&maxBatchSize={maxBatchSize:int|optional}")]
        public async Task Pull()
        {
            var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);

            try
            {
                using (var subscriptionConnection = new SubscriptionWebSocketConnection(ContextPool, webSocket, Database))
                {
                    await subscriptionConnection.InitConnection(GetLongQueryString("id"), GetStringQueryString("connection"), GetStringQueryString("Strategy"), GetIntValueQueryString("maxDocsPerBatch"), GetIntValueQueryString("maxBatchSize",false));
                    await subscriptionConnection.Proccess();
                }
            }
            catch (Exception e)
            {
                try
                {
                    await webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError,
                        // yuck
                        "{'Type':'Error', 'Exception':'" + e.ToString().Replace("'", "\\'") + "'}", Database.DatabaseShutdown).ConfigureAwait(false);
                }
                catch
                {
                    // ignored
                }

                Log.ErrorException($"Failure in subscription id {GetLongQueryString("id")}", e);
            }
            finally
            {
                webSocket.Dispose();
            }
        }

        [RavenAction("/databases/*/subscriptions", "GET", "/databases/{databaseName:string}/subscriptions?start={start:int}&pageSize={pageSize:int}")]
        public Task Get()
        {
            var start = GetStart();
            var take = GetPageSize(Database.Configuration.Core.MaxPageSize);
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                var subscriptionTableValues = Database.SubscriptionStorage.GetSubscriptions(start, take);
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    Database.SubscriptionStorage.WriteSubscriptionTableValues(writer, context, subscriptionTableValues);
                }
            }
            HttpContext.Response.StatusCode = 200;
            return Task.CompletedTask;
        }
    }
}
