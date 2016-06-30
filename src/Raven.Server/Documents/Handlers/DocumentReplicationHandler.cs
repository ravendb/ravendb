using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Server.Documents.Replication;
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
        [RavenAction("/databases/*/documentReplication/changeVector", "GET",
            @"@/databases/{databaseName:string}/documentReplication/changeVector")]
        public Task GetChangeVector()
        {
            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var serverChangeVector = Database.DocumentsStorage.GetChangeVector(context);
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    writer.WriteChangeVector(context, serverChangeVector);
            }

            return Task.CompletedTask;
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
            var srcUrl = HttpContext.Request.GetHostnameUrl();

            var ReplicationReceiveDebugTag = $"document-replication/receive <{Database.DocumentReplicationLoader.ReplicationUniqueName}>";
            var incomingReplication = new IncomingDocumentReplication(Database);
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            using (var webSocketStream = new WebsocketStream(webSocket,Database.DatabaseShutdown))
            {
                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                using (context)
                {
                    var buffer = new ArraySegment<byte>(context.GetManagedBuffer());
                    var jsonParserState = new JsonParserState();
                    using (var parser = new UnmanagedJsonParser(context, jsonParserState, ReplicationReceiveDebugTag))
                    {
                        while (!Database.DatabaseShutdown.IsCancellationRequested)
                        {
                            //this loop handles one replication batch
                            try
                            {
                                var result = await webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
                                if (result.CloseStatus != null)
                                    break;

                                //open write transaction at beginning of the batch
                                using (var writer = new BlittableJsonDocumentBuilder(context,
                                    BlittableJsonDocumentBuilder.UsageMode.None, ReplicationReceiveDebugTag,
                                    parser, jsonParserState))
                                {
                                    writer.ReadObject();
                                    parser.SetBuffer(buffer.Array, result.Count);
                                    while (writer.Read() == false)
                                    {
                                        result = await webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
                                        parser.SetBuffer(buffer.Array, result.Count);
                                    }
                                    writer.FinalizeDocument();
                                    var message = writer.CreateReader();
                                    string messageTypeAsString;
                                    if (!message.TryGet(Constants.MessageType, out messageTypeAsString))
                                        throw new InvalidDataException(
                                            $"Got websocket message without a type. Expected property with name {Constants.MessageType}, but found none.");

                                    HandleMessage(
                                        messageTypeAsString,
                                        message,
                                        webSocketStream,
                                        context,
                                        srcDbId,
                                        incomingReplication);
                                }
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException($@"Failed to receive replication document batch. (Origin -> Database Id = {srcDbId}, Database Name = {srcDbName}, Origin URL = {srcUrl})", e);
                            }
                        }
                    }
                }
            }
        }

        private void HandleMessage(string messageTypeAsString, 
            BlittableJsonReaderObject message, 
            WebsocketStream webSocketStream, 
            DocumentsOperationContext context, 
            Guid srcDbId, 
            IncomingDocumentReplication incomingReplication)
        {
            switch (messageTypeAsString)
            {
                case Constants.Replication.MessageTypes.GetLastEtag:
                    using (context.OpenReadTransaction())
                        WriteLastEtagResponse(webSocketStream, context, srcDbId);
                    break;
                case Constants.Replication.MessageTypes.ReplicationBatch:
                    BlittableJsonReaderArray replicatedDocs;
                    if (!message.TryGet(Constants.Replication.PropertyNames.ReplicationBatch, out replicatedDocs))
                        throw new InvalidDataException(
                            $"Expected the message to have a field with replicated document array, named {Constants.Replication.PropertyNames.ReplicationBatch}. The property wasn't found");
                    using (context.OpenWriteTransaction())
                    {
                        incomingReplication.ReceiveDocuments(context, replicatedDocs);
                        context.Transaction.Commit();
                    }
                    break;
                default:
                    throw new NotSupportedException($"Received not supported message type : {messageTypeAsString}");
            }
        }

        //NOTE : assumes at least read transaction open in the context
        private long GetLastReceivedEtag(Guid srcDbId, DocumentsOperationContext context)
        {
            var serverChangeVector = Database.DocumentsStorage.GetChangeVector(context);
            var vectorEntry = serverChangeVector.FirstOrDefault(x => x.DbId == srcDbId);
            return vectorEntry.Etag;
        }

        private void WriteLastEtagResponse(WebsocketStream webSocketStream, DocumentsOperationContext context, Guid srcDbId)
        {           			
            using (var responseWriter = new BlittableJsonTextWriter(context, webSocketStream))
            {
                context.Write(responseWriter, new DynamicJsonValue
                {
                    [Constants.Replication.PropertyNames.LastSentEtag] = GetLastReceivedEtag(srcDbId, context),
                    [Constants.MessageType] = Constants.Replication.MessageTypes.GetLastEtag
                });
                responseWriter.Flush();
            }
        }
    }
}
