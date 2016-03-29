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
using Lucene.Net.Spatial.Util;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Bits = Sparrow.Binary.Bits;

namespace Raven.Server.Documents.Handlers
{
    public class BulkInsertHandler : DatabaseRequestHandler
    {
        private readonly BlockingCollection<BulkBufferInfo> _fullBuffers = new BlockingCollection<BulkBufferInfo>();
        private readonly BlockingCollection<BulkBufferInfo> _freeBuffers = new BlockingCollection<BulkBufferInfo>();

        public enum ResponseMessageType
        {
            Ok,
            Error
        }

        private unsafe class BulkBufferInfo
        {
            public int Used;
            public UnmanagedBuffersPool.AllocatedMemoryData Buffer;
            public List<int> Sizes;
        }

        private static readonly ArraySegment<byte> HeartbeatMessage = new ArraySegment<byte>(Encoding.UTF8.GetBytes("Heartbeat"));

        private WebSocket _webSocket;

        public unsafe void InsertDocuments()
        {
            var lastHeartbeat = SystemTime.UtcNow;
            try
            {
                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    while (_fullBuffers.IsCompleted == false)
                    {
                        BulkBufferInfo current;
                        try
                        {
                            current = _fullBuffers.Take(Database.DatabaseShutdown);
                        }
                        catch (InvalidOperationException) // adding completed
                        {
                            break;
                        }
                        if (Log.IsDebugEnabled)
                            Log.Debug($"Starting bulk insert batch with {current.Sizes.Count} documents of size {current.Used} bytes");
                        var sp = Stopwatch.StartNew();
                        using (var tx = context.OpenWriteTransaction())
                        {
                            byte* docPtr = (byte*)current.Buffer.Address;
                            for (int i = 0; i < current.Sizes.Count; i++)
                            {
                                var size = current.Sizes[i];
                                var reader = new BlittableJsonReaderObject(docPtr,size, context);
                                docPtr += size;
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
                            _freeBuffers.Add(current);
                            tx.Commit();
                        }
                        lastHeartbeat = SendHeartbeatIfNecessary(lastHeartbeat);
                        if (Log.IsDebugEnabled)
                            Log.Debug($"Completed bulk insert batch with {current.Sizes.Count} documents in {sp.ElapsedMilliseconds:#,#;;0} ms");

                    }
                }
            }
            catch (Exception)
            {
                _fullBuffers.CompleteAdding();
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
                    }, TaskContinuationOptions.OnlyOnFaulted);
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

                for (int i = 0; i < 8; i++)
                {
                    _freeBuffers.Add(new BulkBufferInfo
                    {
                        Sizes = new List<int>(),
                        Buffer = context.GetMemory(1024 * 1024 * 4)
                    });
                }

                var buffer = new ArraySegment<byte>(context.GetManagedBuffer());
                var state = new JsonParserState();
                var task = Task.Factory.StartNew(InsertDocuments);
                try
                {
                    var current = _freeBuffers.Take();
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
                            using (var doc = new BlittableJsonDocumentBuilder(context,
                                BlittableJsonDocumentBuilder.UsageMode.CompressStrings | 
                                BlittableJsonDocumentBuilder.UsageMode.ValidateDouble,
                                bulkInsertDocumentDebugTag,
                                parser, state))
                            {
                                doc.ReadObject();
                                while (doc.Read() == false) //received partial document
                                {
                                    if (_webSocket.State != WebSocketState.Open)
                                        break;
                                    result = await _webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
                                    parser.SetBuffer(new ArraySegment<byte>(buffer.Array, buffer.Offset,
                                        result.Count));
                                }

                                if (_webSocket.State == WebSocketState.Open)
                                {
                                    doc.FinalizeDocument();
                                    count++;
                                    if (current.Used + doc.SizeInBytes > current.Buffer.SizeInBytes)
                                    {
                                        try
                                        {
                                            _fullBuffers.Add(current);
                                        }
                                        catch (Exception)
                                        {
                                            break;// error in the actual insert, we'll get it when we await on the insert task
                                        }
                                        current = _freeBuffers.Take();
                                        if (current.Buffer.SizeInBytes < doc.SizeInBytes)
                                        {
                                            context.ReturnMemory(current.Buffer);
                                            current.Buffer = context.GetMemory(Bits.NextPowerOf2(doc.SizeInBytes));
                                        }
                                        current.Sizes.Clear();
                                        current.Used = 0;
                                    }
                                    doc.CopyTo(current.Buffer.Address + current.Used);
                                    current.Used += doc.SizeInBytes;
                                    current.Sizes.Add(doc.SizeInBytes);
                                }
                            }
                            if (result.EndOfMessage)
                                break;
                        }
                        try
                        {
                            _fullBuffers.Add(current);
                        }
                        catch (Exception)
                        {
                            // error in the insert, we'll get it when we await on the insert task
                        }
                        _fullBuffers.CompleteAdding();
                    }
                    await task;
                    var msg = $"Successfully bulk inserted {count} documents in {sp.ElapsedMilliseconds:#,#;;0} ms";
                    Log.Debug(msg);
                    await _webSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, msg, CancellationToken.None);
                }
                catch (Exception e)
                {
                    _fullBuffers.CompleteAdding();
                    try
                    {
                        await _webSocket.CloseOutputAsync(WebSocketCloseStatus.InternalServerError,
                            e.ToString(), Database.DatabaseShutdown);
                    }
                    catch (Exception)
                    {
                        // nothing to do further
                    }
                    Log.ErrorException("Failure in bulk insert", e);
                }
            }
        }
    }
}