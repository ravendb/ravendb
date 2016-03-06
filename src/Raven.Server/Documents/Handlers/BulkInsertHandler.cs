// -----------------------------------------------------------------------
//  <copyright file="BulkInsertHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
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
        private readonly BlockingCollection<BlittableJsonReaderObject> _docs;

        public enum ResponseMessageType
        {
            Ok,
            Error
        }

        public BulkInsertHandler()
        {
            _docs = new BlockingCollection<BlittableJsonReaderObject>(512);
        }

        public void InsertDocuments()
        {
            try
            {
                var buffer = new List<BlittableJsonReaderObject>(_docs.BoundedCapacity);
                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    while (_docs.IsCompleted == false)
                    {
                        buffer.Clear();
                        var doc = _docs.Take(Database.DatabaseShutdown);
                        buffer.Add(doc);
                        while (_docs.TryTake(out doc))
                        {
                            buffer.Add(doc);
                        }

                        using (var tx = context.OpenWriteTransaction())
                        {
                            foreach (var reader in buffer)
                            {
                                string docKey;
                                BlittableJsonReaderObject metadata;
                                const string idKey = "@id";
                                if (reader.TryGet(Constants.Metadata, out metadata) == false ||
                                    metadata.TryGet(idKey, out docKey) == false)
                                {
                                    const string message = "bad doc key";
                                    throw new InvalidDataException(message);
                                }
                                Database.DocumentsStorage.Put(context, docKey, 0, reader);
                            }
                            tx.Commit();
                        }
                    }
                }
            }
            catch (Exception)
            {
                _docs.CompleteAdding();				
                throw;
            }
        }

        [RavenAction("/databases/*/bulkInsert", "GET", "/databases/{databaseName:string}/bulkInsert")]
        public async Task BulkInsert()
        {
            DocumentsOperationContext context;
            var hasAlreadyThrown = false;
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            using (ContextPool.AllocateOperationContext(out context))
            {
                var buffer = new ArraySegment<byte>(context.GetManagedBuffer());
                var state = new JsonParserState();
                var task = Task.Factory.StartNew(InsertDocuments);
                try
                {
                    const string bulkInsertDebugTag = "bulk/insert";
                    using (var parser = new UnmanagedJsonParser(context, state, bulkInsertDebugTag))
                    {
                        var result = await webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
                        await SendResponse(webSocket, ResponseMessageType.Ok);
                        parser.SetBuffer(new ArraySegment<byte>(buffer.Array, buffer.Offset,
                            result.Count));

                        while (true)
                        {
                            const string bulkInsertDocumentDebugTag = "bulk/insert/document";
                            var doc = new BlittableJsonDocumentBuilder(context,
                                BlittableJsonDocumentBuilder.UsageMode.ToDisk,
                                bulkInsertDocumentDebugTag,
                                parser, state);

                            doc.ReadObject();
                            while (doc.Read() == false) //received partial document
                            {
                                result = await webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
                                await SendResponse(webSocket, ResponseMessageType.Ok);
                                parser.SetBuffer(new ArraySegment<byte>(buffer.Array, buffer.Offset,
                                    result.Count));
                            }
                            doc.FinalizeDocument();

                            var reader = doc.CreateReader();
                            try
                            {
                                _docs.Add(reader);
                            }
                            catch (InvalidOperationException)
                            {
                                // error in actual insert, abort
                                // actual handling is done below
                                break;
                            }
                            if (result.EndOfMessage)
                                break;
                        }
                        _docs.CompleteAdding();
                    }
                }
                catch (Exception e)
                {
                    _docs.CompleteAdding();
                    await SendResponse(webSocket, ResponseMessageType.Error);
                    hasAlreadyThrown = true;
                    //TODO : add logging
                    throw;
                }
                finally
                {
                    const string bulkinsertFinishedMessage = "BulkInsert Finished";
                    await task;
                    if(task.IsFaulted && !hasAlreadyThrown)
                        await SendResponse(webSocket, ResponseMessageType.Error);
                    await webSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, bulkinsertFinishedMessage, CancellationToken.None);
                }
            }
        }

        private static readonly ArraySegment<byte> _okResponse = new ArraySegment<byte>(new byte[] { 0 });
        private static readonly ArraySegment<byte> _errorResponse = new ArraySegment<byte>(new byte[] { 1 });

        private Task SendResponse(WebSocket webSocket, ResponseMessageType messageType)
        {
            return webSocket.SendAsync(
                messageType == ResponseMessageType.Ok ? 
                    _okResponse : _errorResponse, 
                WebSocketMessageType.Binary, true,
                Database.DatabaseShutdown);
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