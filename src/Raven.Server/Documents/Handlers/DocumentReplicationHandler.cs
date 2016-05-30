using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentReplicationRequestHandler : DatabaseRequestHandler
    {
        private long GetLastEtag(Guid srcDbId, DocumentsOperationContext context)
        {
            RavenTransaction tx = null;

            try
            {
                if (context.Transaction == null || context.Transaction.Disposed)
                    tx = context.OpenReadTransaction();
                var serverChangeVector = Database.DocumentsStorage.GetChangeVector(context);
                var vectorEntry = serverChangeVector.FirstOrDefault(x => x.DbId == srcDbId);
                return vectorEntry.Etag;
            }
            finally
            {
                if(tx != null || context.Transaction.Disposed == false)
                    tx?.Dispose();
            }
        }

        //an endpoint to establish replication websocket
        [RavenAction("/databases/*/documentReplication", "GET",
            @"@/databases/{databaseName:string}/documentReplication?
                srcDbId={databaseUniqueId:string}
                &srcDbName={databaseName:string}")]
        public async Task DocumentReplicationConnection()
        {
            var srcDbId = Guid.Parse(GetQueryStringValueAndAssertIfSingleAndNotEmpty("srcDbId"));
            var srcDbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("srcDbName");
            long lastSentEtag;
            if (!long.TryParse(GetQueryStringValueAndAssertIfSingleAndNotEmpty("lastSentEtag"), out lastSentEtag))
            {
                throw new ArgumentException("lastSentEtag should be a Int64 number, failed to parse...");
            }

            var hasOtherSideClosedConnection = false;
            var srcUrl = HttpContext.Request.GetHostnameUrl();
            var executer = Database.DocumentReplicationLoader.RegisterConnection(
                srcDbId,
                srcUrl,
                srcDbName);

            if(executer.HasOutgoingReplication)
                executer.Start();

            string ReplicationReceiveDebugTag = $"document-replication/receive <{executer.ReplicationUniqueName}>";			

            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    var docs = new List<BlittableJsonReaderObject>();
                    var buffer = new ArraySegment<byte>(context.GetManagedBuffer());
                    var jsonParserState = new JsonParserState();
                    using (var parser = new UnmanagedJsonParser(context, jsonParserState, ReplicationReceiveDebugTag))
                    {
                        while (!Database.DatabaseShutdown.IsCancellationRequested)
                        {
                            //this loop handles one replication batch
                            RavenTransaction tx = null;
                            try
                            {
                                while (true)
                                {
                                    var result = await webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
                                    //open write transaction at beginning of the batch
                                    if (tx == null)
                                        tx = context.OpenWriteTransaction();

                                    var writer = new BlittableJsonDocumentBuilder(context,
                                        BlittableJsonDocumentBuilder.UsageMode.None, ReplicationReceiveDebugTag,
                                        parser, jsonParserState);
                                    writer.ReadObject();
                                    if (result.MessageType == WebSocketMessageType.Close)
                                    {
                                        hasOtherSideClosedConnection = true;
                                        break;
                                    }

                                    if (result.EndOfMessage)
                                        break;

                                    parser.SetBuffer(buffer.Array, result.Count);
                                    while (writer.Read() == false)
                                    {
                                        result = await webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
                                        parser.SetBuffer(buffer.Array, result.Count);
                                    }
                                    writer.FinalizeDocument();
                                    var receivedDoc = writer.CreateReader();

                                    //special document that signifies "Get Last Etag" request
                                    if (receivedDoc.Count == 1 && receivedDoc.GetPropertyNames()[0] == "Raven/GetLastEtag")
                                    {
                                        WriteLastEtagResponse(webSocket, context, srcDbId);
                                        continue;
                                    }
                                    docs.Add(receivedDoc);
                                }
                                executer.ReceiveReplicatedDocuments(context, docs);

                                //precaution
                                if (tx == null)
                                    throw new InvalidOperationException(@"
                                        Transaction is not initialized while receiving replicated documents; this
                                         is something that is not supposed to happen and is likely a bug.");

                                tx.Commit();
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException($@"Failed to receive replication document batch. (Origin -> Database Id = {srcDbId}, Database Name = {srcDbName}, Origin URL = {srcUrl})", e);
                            }
                            finally
                            {
                                tx?.Dispose();
                            }

                            if (hasOtherSideClosedConnection)
                                break;
                        }
                    }

                    //if execution path gets here, it means the node was disconnected
                    Database.DocumentReplicationLoader.HandleConnectionDisconnection(executer);
                }
            }
        }

        private void WriteLastEtagResponse(WebSocket webSocket, DocumentsOperationContext context, Guid srcDbId)
        {
            using (var websocketStream = new WebsocketStream(webSocket, Database.DatabaseShutdown))
            using (var responseWriter = new BlittableJsonTextWriter(context, websocketStream))
            {
                context.Write(responseWriter, new DynamicJsonValue
                {
                    ["Raven/LastSentEtag"] = GetLastEtag(srcDbId, context)
                });
                responseWriter.Flush();
            }
        }
    }
}
