using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentReplicationRequestHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases*/lastSentChangeVector", "GET", "@/databases/{databaseName: string}/lastSentChangeVector")]
        public async Task GetLastSentChangeVector()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
            }
        }

        //an endpoint to establish replication websocket
        [RavenAction("/databases/*/documentReplication", "GET",
            @"/databases/{databaseName:string}/documentReplication?
                srcDbId={databaseUniqueId:string}
                &srcUrl={url:string}
                &srcDbName={databaseName:string}")]
        public async Task DocumentReplicationConnection()
        {
            var dbId = Guid.Parse(GetQueryStringValueAndAssertIfSingleAndNotEmpty("srcDbId")[0]);
            var srcUrl = GetQueryStringValueAndAssertIfSingleAndNotEmpty("srcUrl")[0];
            var srcDbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("srcDbName")[0];

            var hasOtherSideClosedConnection = false;
            bool shouldConnectBack;
            var replicationExecuter = Database.DocumentReplicationLoader.RegisterNewConnectionFrom(
                dbId,
                srcUrl,
                srcDbName,
                out shouldConnectBack);
            replicationExecuter.Start();

            string ReplicationReceiveDebugTag = $"document-replication/receive <{replicationExecuter.Name}>";

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
                            while (true)
                            {

                                var writer = new BlittableJsonDocumentBuilder(context,
                                    BlittableJsonDocumentBuilder.UsageMode.None, ReplicationReceiveDebugTag,
                                    parser, jsonParserState);
                                writer.ReadObject();
                                var result = await webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);

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
                                docs.Add(receivedDoc);
                            }

                            replicationExecuter.ReceiveReplicatedDocuments(context, docs);

                            if (hasOtherSideClosedConnection)
                                break;
                        }
                    }

                    //if execution path gets here, it means the node was disconnected
                    Database.DocumentReplicationLoader.HandleConnectionDisconnection(replicationExecuter);
                }
            }
        }


        private bool IsCommandDocument(BlittableJsonReaderObject doc)
        {
            string val;
            var hasProperty = doc.TryGet(Constants.TransportResponseProperty, out val);
            return hasProperty && !String.IsNullOrWhiteSpace(val);
        }
    }
}
