// -----------------------------------------------------------------------
//  <copyright file="BulkInsertHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class BulkInsertHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulkInsert", "GET", "/databases/{databaseName:string}/bulkInsert")]
        public async Task BulkInsert()
        {
            // TODO: expose current (and recent) bulk insert on /debug/bulk-inserts
            // TODO: allow to cancel this via /debug/bulk-inserts
            // var tcs = new CancellationTokenSource();
            DocumentsOperationContext context;
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            using (ContextPool.AllocateOperationContext(out context))
            {
                var buffer = context.GetManagedBuffer();
                var segments = new[]
                {
                    new ArraySegment<byte>(buffer,0, buffer.Length/2),
                    new ArraySegment<byte>(buffer,buffer.Length/2, buffer.Length/2)
                };
                int index = 0;
                var receiveAsync = webSocket.ReceiveAsync(segments[index], Database.DatabaseShutdown);

                // bulk insert format is a sequence of documents 
                // in json format that are being send over the wire
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(context, state, "bulk/insert"))
                {
                    int size = 0;
                    var docs = new List<BlittableJsonDocumentBuilder>();
                    while (true)
                    {
                        var result = await receiveAsync;

                        // TODO: error handling, how do we know when we are closed?
                        if (result.CloseStatus == WebSocketCloseStatus.NormalClosure)
                            break;

                        if (result.CloseStatus != null && 
                            result.CloseStatus != WebSocketCloseStatus.NormalClosure)
                            throw new InvalidOperationException($"The websocket is closed, but with error (CloseStatus = {result.CloseStatus}). This is not supposed to happen and is likely a bug.");

                        parser.SetBuffer(new ArraySegment<byte>(buffer, segments[index].Offset, result.Count));

                        if (++index >= segments.Length)
                            index = 0;
                        receiveAsync = webSocket.ReceiveAsync(segments[index], Database.DatabaseShutdown);

                        var doc = new BlittableJsonDocumentBuilder(context,
                            BlittableJsonDocumentBuilder.UsageMode.ToDisk,
                            "bulk/insert/document",
                            parser, state);

                        doc.ReadObject();
                        while (doc.Read() == false)
                        {
                            result = await receiveAsync;
                            parser.SetBuffer(new ArraySegment<byte>(buffer, segments[index].Offset, result.Count));

                            if (++index >= segments.Length)
                                index = 0;
                            receiveAsync = webSocket.ReceiveAsync(segments[index], Database.DatabaseShutdown);
                        }
                        docs.Add(doc);

                        size += doc.SizeInBytes;

                        if (size > 1024 * 1024 * 16)// flush every 16 MB or so
                        {
                            FlushDocuments(context, docs);
                            size = 0;
                            docs.Clear();
                        }
                    }
                    FlushDocuments(context, docs);
                }

                await webSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "BulkInsert Finished", CancellationToken.None);
            }
        }

        private void FlushDocuments(DocumentsOperationContext context, List<BlittableJsonDocumentBuilder> docs)
        {
            using (var tx = context.OpenWriteTransaction())
            {
                foreach (var builder in docs)
                {
                    using (var reader = builder.CreateReader())
                    {
                        string docKey;
                        BlittableJsonReaderObject metadata;
                        if (reader.TryGet(Constants.Metadata, out metadata) == false ||
                            reader.TryGet("@id", out docKey) == false)
                        {
                            // could not read key
                            // TODO: Send error to the client and close connection
                            throw new InvalidDataException("bad doc key");
                        }
                        Database.DocumentsStorage.Put(context, docKey, 0, reader);
                    }
                }
                //TODO: Error handling
                tx.Commit();
            }
        }


        public class BulkInsertStatus //TODO: implements operations state : IOperationState
        {
            public int Documents { get; set; }
            public bool Completed { get; set; }

            public bool Faulted { get; set; }

            //TODO: report state
            //public RavenJToken State { get; set; }

            public bool IsTimedOut { get; set; }

            public bool IsSerializationError { get; set; }
        }
    }
}