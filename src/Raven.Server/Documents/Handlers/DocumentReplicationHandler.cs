using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentReplicationRequestHandler : DatabaseRequestHandler
    {
        //an endpoint to establish replication websocket
        [RavenAction("/databases/*/documentReplication", "GET",
            @"/databases/{databaseName:string}/documentReplication?
                srcDbId={databaseUniqueId:string}
                &srcUrl={url:string}
                &srcDbName={databaseName:string}
                &lastSentEtag={etag:long}")]
        public async Task DocumentReplicationConnection()
        {
            var dbId = Guid.Parse(GetQueryStringValueAndAssertIfSingleAndNotEmpty("srcDbId"));
            var srcUrl = GetQueryStringValueAndAssertIfSingleAndNotEmpty("srcUrl");
            var srcDbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("srcDbName");
            long lastSentEtag;
            if (!long.TryParse(GetQueryStringValueAndAssertIfSingleAndNotEmpty("lastSentEtag"), out lastSentEtag))
            {
                throw new ArgumentException("lastSentEtag should be a Int64 number, failed to parse...");
            }

            bool hasOtherSideClosedConnection = false;
            bool shouldConnectBack;
            var replicationExecuter = Database.DocumentReplicationLoader.RegisterNewConnectionFrom(dbId,
                srcUrl,
                srcDbName,
                lastSentEtag,
                out shouldConnectBack);
            string ReplicationReceiveDebugTag = $"document-replication/receive <{replicationExecuter.Name}>";

            using (var timeoutEvent = new ManualResetEventSlim())
            {
                var timeoutTimeSpan = Database.Configuration.Core.DatabaseOperationTimeout.AsTimeSpan;
                using (var timeoutTimer = new Timer(_ => timeoutEvent.Set(), null, timeoutTimeSpan, Timeout.InfiniteTimeSpan))
                using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
                {
                    DocumentsOperationContext context;
                    using (ContextPool.AllocateOperationContext(out context))
                    {
                        var docs = new List<BlittableJsonReaderObject>();
                        var buffer = new ArraySegment<byte>(context.GetManagedBuffer());
                        while (!Database.DatabaseShutdown.IsCancellationRequested && !timeoutEvent.IsSet)
                        {
                            //this loop handles one replication batch
                            while (true)
                            {
                                var jsonParserState = new JsonParserState();
                                using (var parser = new UnmanagedJsonParser(context, jsonParserState, ReplicationReceiveDebugTag))
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
                                    docs.Add(writer.CreateReader());
                                }
                            }
                            timeoutTimer.Change(timeoutTimeSpan, Timeout.InfiniteTimeSpan);
                            replicationExecuter.ReceiveReplicatedDocuments(context, docs);

                            if (hasOtherSideClosedConnection)
                                break;
                        }
                    }
                }
            }
        }
    }
}
