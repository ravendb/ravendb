// -----------------------------------------------------------------------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class ChangesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/changes", "GET", "/databases/{databaseName:string}/changes")]
        public async Task GetChangesEvents()
        {
            var debugTag = "changes/" + Database.Name;

            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                RavenOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    var buffer = context.GetManagedBuffer();
                    var receiveAsync = webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), ServerStore.ServerShutdown);
                    var jsonParserState = new JsonParserState();
                    using (var parser = new UnmanagedJsonParser(context, jsonParserState, debugTag))
                    {
                        var result = await receiveAsync;
                        receiveAsync = webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), ServerStore.ServerShutdown);
                        parser.SetBuffer(buffer, result.Count);

                        while (true)
                        {
                            using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState))
                            {
                                while (builder.Read() == false)
                                {
                                    result = await receiveAsync;
                                    receiveAsync = webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), ServerStore.ServerShutdown);
                                    parser.SetBuffer(buffer, result.Count);
                                }

                                var changesConfigRequest = builder.CreateReader();
                                Database.
                            }
                        }
                    }
                }
            }
        }
    }
}