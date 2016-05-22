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

        private static readonly ILog log = LogManager.GetLogger(typeof(SubscriptionsHandler));

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

        [RavenAction("/databases/*/subscriptions/open", "POST",
            "/databases/{databaseName:string}/subscriptions/open?id={subscriptionId:long}")]
        public Task Open()
        {
            var id = GetLongQueryString("id");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                Database.SubscriptionStorage.AssertSubscriptionConfigExists(id.Value);
                var options = context.ReadForDisk(RequestBodyStream(), "Subscriptions");
                Database.SubscriptionStorage.OpenSubscription(id.Value, options);
            }

            return Task.CompletedTask;
        }



        private unsafe BlittableJsonReaderObject GetReaderObjectFromTableReader(TableValueReader tbl, int index, DocumentsOperationContext context)
        {
            int criteriaSize;
            var criteriaPtr = tbl.Read(index, out criteriaSize);
            return new BlittableJsonReaderObject(criteriaPtr, criteriaSize, context);
        }

        private unsafe long GetLongValueFromTableReader(TableValueReader tbl, int index)
        {
            int size;
            return *(long*)tbl.Read(index, out size);
        }

        private bool MatchCriteria(SubscriptionCriteria criteria, Document doc)
        {
            // todo: implement
            return true;
        }

        public static long DocumentsPullTimeoutMiliseconds = 1000;

        private static Task<WebSocketReceiveResult> _webSocketReceiveCompletedTask = Task.FromResult((WebSocketReceiveResult)null);


        private async Task<WebSocketReceiveResult> ReadFromWebSocketWithKeepAlives(WebSocket ws, ArraySegment<byte> clientAckBuffer, MemoryStream ms)
        {
            var receiveAckTask = ws.ReceiveAsync(clientAckBuffer, Database.DatabaseShutdown);
            while (Task.WhenAny(receiveAckTask, Task.Delay(5000)) != null &&
                (receiveAckTask.IsCompleted || receiveAckTask.IsFaulted ||
                    receiveAckTask.IsCanceled) == false)
            {
                ms.WriteByte((byte)'\r');
                ms.WriteByte((byte)'\n');
                // just to keep the heartbeat
                await FlushStreamToClient(ms, ws, Database.DatabaseShutdown).ConfigureAwait(false);
            }

            return await receiveAckTask.ConfigureAwait(false);
        }

        [RavenAction("/databases/*/subscriptions/close", "POST",
            "/databases/{databaseName:string}/subscriptions/close?id={subscriptionId:long}&connection={connection:string}&force={force:bool|optional}"
            )]
        public async Task Close()
        {
            var id = GetLongQueryString("id");
            var connection = GetStringQueryString("connection");
            bool force = GetBoolValueQueryString("force", required: false)?? false;

            if (force == false)
            {
                try
                {
                    Database.SubscriptionStorage.AssertOpenSubscriptionConnection(id.Value, connection);
                }
                catch (SubscriptionException)
                {
                    // ignore if assertion exception happened on close
                    return;
                }
            }

            Database.SubscriptionStorage.ReleaseSubscription(id.Value, force);


            return;
        }

        [RavenAction("/databases/*/subscriptions/pull", "GET",
            "/databases/{databaseName:string}/subscriptions/pull?id={subscriptionId:long}&connection={connection:string}"
            )]
        public async Task Pull()
        {
            var id = GetLongQueryString("id");
            var connection = GetStringQueryString("connection");
            Database.SubscriptionStorage.AssertOpenSubscriptionConnection(id.Value, connection);

            var waitForMoreDocuments = new AsyncManualResetEvent();

            var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);

            long lastEtagAcceptedFromClient = 0;
            long lastEtagSentToClient = 0;

            try
            {
                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    Log.Debug("Starting subscription pushing");
                    var clientAckBuffer = new ArraySegment<byte>(context.GetManagedBuffer());

                    var ackParserState = new JsonParserState();
                    using (var ms = new MemoryStream())
                    using (var writer = new BlittableJsonTextWriter(context, ms))
                    using (var ackParser = new UnmanagedJsonParser(context, ackParserState, string.Empty))
                    {
                        var config = Database.SubscriptionStorage.GetSubscriptionConfig(id.Value);
                        var options = Database.SubscriptionStorage.GetSubscriptionOptions(id.Value);

                        var startEtag = GetLongValueFromTableReader(config,
                            SubscriptionStorage.Schema.SubscriptionTable.AckEtagIndex);
                        var criteria = Database.SubscriptionStorage.GetCriteria(id.Value, context);

                        Action<DocumentChangeNotification> registerNotification = notification =>
                        {
                            if (notification.CollectionName == criteria.Collection)
                                waitForMoreDocuments.SetByAsyncCompletion();
                            
                        };
                        Database.Notifications.OnDocumentChange += registerNotification;
                        try
                        {
                            int skipNumber = 0;
                            while (Database.DatabaseShutdown.IsCancellationRequested == false)
                            {
                                int documentsSent = 0;

                                var hasDocuments = false;
                                using (context.OpenReadTransaction())
                                {
                                    var documents = Database.DocumentsStorage.GetDocumentsAfter(context,
                                        criteria.Collection,
                                        startEtag + 1, 0, options.MaxDocCount);

                                    foreach (var doc in documents)
                                    {
                                        hasDocuments = true;
                                        startEtag = doc.Etag;
                                        if (MatchCriteria(criteria, doc) == false)
                                        {
                                            if (skipNumber++ % options.MaxDocCount == 0)
                                            {
                                                ms.WriteByte((byte)'\r');
                                                ms.WriteByte((byte)'\n');
                                            }
                                            continue;
                                        }
                                        documentsSent++;
                                        doc.EnsureMetadata();
                                        context.Write(writer, doc.Data);
                                        lastEtagSentToClient = doc.Etag;
                                        doc.Data.Dispose();
                                    }
                                }

                                if (hasDocuments == false)
                                {
                                    while (await waitForMoreDocuments.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false) == false)
                                    {
                                        ms.WriteByte((byte)'\r');
                                        ms.WriteByte((byte)'\n');
                                        // just to keep the heartbeat
                                        await FlushStreamToClient(ms, webSocket, Database.DatabaseShutdown).ConfigureAwait(false);
                                    }

                                    waitForMoreDocuments.Reset();
                                    continue;
                                }
                                writer.Flush();
                                await FlushStreamToClient(ms, webSocket, Database.DatabaseShutdown, true).ConfigureAwait(false);
                                Database.SubscriptionStorage.UpdateSubscriptionTimes(id.Value, updateLastBatch: true,
                                    updateClientActivity: false);

                                if (documentsSent > 0)
                                {
                                    while (lastEtagAcceptedFromClient < lastEtagSentToClient)
                                    {
                                        using (var builder = new BlittableJsonDocumentBuilder(context,
                                                BlittableJsonDocumentBuilder.UsageMode.None, string.Empty, ackParser,
                                                ackParserState))
                                        {
                                            builder.ReadObject();

                                            while (builder.Read() == false)
                                            {
                                                var result =
                                                    await
                                                        ReadFromWebSocketWithKeepAlives(webSocket, clientAckBuffer, ms).ConfigureAwait(false);
                                                ackParser.SetBuffer(new ArraySegment<byte>(clientAckBuffer.Array, 0,
                                                    result.Count));
                                            }

                                            builder.FinalizeDocument();

                                            using (var reader = builder.CreateReader())
                                            {
                                                if (reader.TryGet("LastEtag", out lastEtagAcceptedFromClient) == false)
                                                    // ReSharper disable once NotResolvedInText
                                                    throw new ArgumentNullException("LastEtag");
                                            }
                                        }
                                    }

                                    Database.SubscriptionStorage.UpdateSubscriptionTimes(id.Value, updateLastBatch: false,
                                        updateClientActivity: true);
                                    Database.SubscriptionStorage.AcknowledgeBatchProcessed(id.Value, startEtag);
                                }
                            }
                        }
                        finally
                        {
                            Database.Notifications.OnDocumentChange -= registerNotification;
                        }


                    }

                }
            }
            catch (Exception e)
            {
                try
                {
                    var cancellationCts = new CancellationTokenSource();
                    var shudownTimeout = new CancellationTimeout(cancellationCts,
                        TimeSpan.FromSeconds(5));
                    await webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError,
                        e.ToString(), cancellationCts.Token).ConfigureAwait(false);
                }
                catch
                {
                    // ignored
                }

                Log.ErrorException($"Failure in subscription id {id}", e);
            }
            finally
            {
                webSocket.Dispose();
            }
        }

        private static async Task FlushStreamToClient(MemoryStream ms, WebSocket webSocket, CancellationToken ct, bool endMessage = false)
        {
            ArraySegment<byte> bytes;
            ms.TryGetBuffer(out bytes);
            await webSocket.SendAsync(bytes, WebSocketMessageType.Text, endMessage, ct).ConfigureAwait(false);
            ms.SetLength(0);
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

        [RavenAction("/databases/*/subscriptions/client-alive", "PATCH", "/databases/{databaseName:string}/subscriptions/client-alive?id={id:string}&connection={connection:string}")]
        public Task ClientAlive()
        {
            var id = GetLongQueryString("id");
            var connection = GetStringQueryString("connection");

            Database.SubscriptionStorage.AssertOpenSubscriptionConnection(id.Value, connection);
            Database.SubscriptionStorage.UpdateSubscriptionTimes(id.Value, updateClientActivity: true, updateLastBatch: false);
            return Task.CompletedTask;
        }
    }
}
