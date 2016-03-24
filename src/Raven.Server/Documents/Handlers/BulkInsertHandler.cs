// -----------------------------------------------------------------------
//  <copyright file="BulkInsertHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Indexes.Persistance.Lucene;
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

        private static readonly ArraySegment<byte> HeartbeatMessage = new ArraySegment<byte>(Encoding.UTF8.GetBytes("Heartbeat"));

        private WebSocket _webSocket;

        public void InsertDocuments()
        {
            var lastHeartbeat = SystemTime.UtcNow;
            try
            {
                var buffer = new List<BlittableJsonReaderObject>(_docs.BoundedCapacity);
                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    while (_docs.IsCompleted == false)
                    {
                        buffer.Clear();
                        BlittableJsonReaderObject doc;
                        try
                        {
                            doc = _docs.Take(Database.DatabaseShutdown);
                        }
                        catch (InvalidOperationException) // adding completed
                        {
                            break;
                        }
                        buffer.Add(doc);
                        int queueSize = 0;
                        while (true)
                        {
                            try
                            {
                                if (_docs.TryTake(out doc) == false)
                                    break;
                            }
                            catch (InvalidOperationException)//adding completed
                            {
                                break;// need to still process the current buffer
                            }
                            queueSize += doc.Size;
                            buffer.Add(doc);
                            if (queueSize >= 1024*1024 * 8) //todo configurable?
                                                            //probably this value needs to be adjusted
                                break;
                        }
                        if(Log.IsDebugEnabled)
                            Log.Debug($"Starting bulk insert batch with {buffer.Count} documents");
                        var sp = Stopwatch.StartNew();
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
                                Database.DocumentsStorage.Put(context, docKey, null, reader);
                            }
                            tx.Commit();
                        }
                        lastHeartbeat = SendHeartbeatIfNecessary(lastHeartbeat);
                        if (Log.IsDebugEnabled)
                            Log.Debug($"Completed bulk insert batch with {buffer.Count} documents in {sp.ElapsedMilliseconds:#,#;;0} ms");

                    }
                }
            }
            catch (Exception)
            {
                _docs.CompleteAdding();				
                throw;
            }
        }

        private DateTime SendHeartbeatIfNecessary(DateTime lastHeartbeat)
        {
            if ((SystemTime.UtcNow - lastHeartbeat).TotalSeconds >= 15)
            {
                _webSocket.SendAsync(HeartbeatMessage, WebSocketMessageType.Text, false,
                    Database.DatabaseShutdown)
                    .ContinueWith(t =>
                    {
                        // ignore the exception, we don't care here
                        // this just force us to call the Exception property, thereby consuming the exception
                        GC.KeepAlive(t.Exception);
                    },TaskContinuationOptions.OnlyOnFaulted);
                lastHeartbeat = SystemTime.UtcNow;
            }

            return lastHeartbeat;
        }

        [RavenAction("/databases/*/bulkInsert", "GET", "/databases/{databaseName:string}/bulkInsert")]
        public async Task BulkInsert()
        {
            DocumentsOperationContext context;
            using (_webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            using (ContextPool.AllocateOperationContext(out context))
            {
                Log.Debug("Starting bulk insert operation");
                var buffer = new ArraySegment<byte>(context.GetManagedBuffer());
                var state = new JsonParserState();
                var task = Task.Factory.StartNew(InsertDocuments);
                try
                {
                    int count = 0;
                    var sp = Stopwatch.StartNew();
                    const string bulkInsertDebugTag = "bulk/insert";
                    using (var parser = new UnmanagedJsonParser(context, state, bulkInsertDebugTag))
                    {
                        var result = await _webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
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
                                if(_webSocket.State != WebSocketState.Open)
                                    break;
                                result = await _webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
                                parser.SetBuffer(new ArraySegment<byte>(buffer.Array, buffer.Offset,
                                    result.Count));
                            }

                            if (_webSocket.State == WebSocketState.Open)
                            {
                                doc.FinalizeDocument();
                                count++;
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
                            }
                            if (result.EndOfMessage)
                                break;
                        }
                        _docs.CompleteAdding();
                    }
                    await task;
                    var msg = $"Successfully bulk inserted {count} documents in {sp.ElapsedMilliseconds:#,#;;0} ms";
                    Log.Debug(msg);
                    await _webSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, msg, CancellationToken.None);
                }
                catch (Exception e)
                {
                    _docs.CompleteAdding();
                    try
                    {
                        await _webSocket.CloseOutputAsync(WebSocketCloseStatus.InternalServerError,
                            e.ToString(), Database.DatabaseShutdown);
                    }
                    catch (Exception)
                    {
                        // nothing to do further
                    }
                    Log.ErrorException("Failure in bulk insert",e);
                }
            }
        }
    }
}