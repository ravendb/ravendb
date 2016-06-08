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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
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

        private class BulkBufferInfo
        {
            public int Used;
            public UnmanagedBuffersPool.AllocatedMemoryData Buffer;
        }

        private static readonly ArraySegment<byte> HeartbeatMessage = new ArraySegment<byte>(Encoding.UTF8.GetBytes("Heartbeat"));

        private WebSocket _webSocket;

        public unsafe void InsertDocuments()
        {
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
                            Log.Debug($"Starting bulk insert batch with size {current.Used:#,#;;0} bytes");
                        int count = 0;
                        var sp = Stopwatch.StartNew();
                        using (var tx = context.OpenWriteTransaction())
                        {
                            tx.InnerTransaction.LowLevelTransaction.IsLazyTransaction = true;

                            byte* docPtr = (byte*)current.Buffer.Address;
                            var end = docPtr + current.Used;
                            while (docPtr < end)
                            {
                                count++;
                                var size = *(int*)docPtr;
                                docPtr += sizeof (int);
                                if (size + docPtr > end) //TODO: Better error
                                    throw new InvalidDataException(
                                        "The blittable size specified is more than the available data, aborting...");
                                //TODO: Paranoid mode, has to validate the data is safe
                                var reader = new BlittableJsonReaderObject(docPtr, size, context);
                                reader.BlittableValidation(size);
                                docPtr += size;
                                string docKey;
                                BlittableJsonReaderObject metadata;
                                
                                if (reader.TryGet(Constants.Metadata, out metadata) == false ||
                                    metadata.TryGet(Constants.MetadataDocId, out docKey) == false)
                                {
                                    const string message = "bad doc key";
                                    throw new InvalidDataException(message);
                                }
                                Database.DocumentsStorage.Put(context, docKey, null, reader);
                            }
                            _freeBuffers.Add(current);
                            tx.Commit();
                        }
                        if (Log.IsDebugEnabled)
                            Log.Debug(
                                $"Completed bulk insert batch with {count} documents in {sp.ElapsedMilliseconds:#,#;;0} ms");

                    }

                    using (var tx = context.OpenWriteTransaction())
                    {
                        // this non lazy transaction forces the journal to actually
                        // flush everything
                        tx.Commit();
                    }
                }
            }
            catch (Exception)
            {
                _fullBuffers.CompleteAdding();
                throw;
            }
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
                        Buffer = context.GetMemory(1024 * 1024 * 4)
                    });
                }

                var buffer = new ArraySegment<byte>(context.GetManagedBuffer());
                var task = Task.Factory.StartNew(InsertDocuments);
                try
                {
                    const string BulkInsertDocumentDebugTag = "bulk/insert/document";
                    using (var stream = context.GetStream())
                    {
                        var current = _freeBuffers.Take();
                        int count = 0;
                        var sp = Stopwatch.StartNew();
                        while (true)
                        {
                            var result = await _webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
                            stream.Write(buffer.Array, 0, result.Count);
                            if (result.EndOfMessage == false)
                                continue;

                            count++;
                            if (current.Used + stream.SizeInBytes > current.Buffer.SizeInBytes)
                            {
                                try
                                {
                                    _fullBuffers.Add(current);
                                }
                                catch (Exception)
                                {
                                    break;
                                    // error in the actual insert, we'll get it when we await on the insert task
                                }
                                current = _freeBuffers.Take();
                                if (current.Buffer.SizeInBytes < stream.SizeInBytes)
                                {
                                    context.ReturnMemory(current.Buffer);
                                    current.Buffer = context.GetMemory(Bits.NextPowerOf2(stream.SizeInBytes));
                                }
                                current.Used = 0;
                            }
                            stream.CopyTo(current.Buffer.Address + current.Used);
                            current.Used += stream.SizeInBytes;
                            stream.Clear();
                            if (result.CloseStatus != null)
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
                        
                        while (true)
                        {
                            var res = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5)));
                            if (res == task)
                                break;
                            // send heartbeat to client so we'll keep the connection open
                            await _webSocket.SendAsync(HeartbeatMessage, WebSocketMessageType.Text, false,
                                    Database.DatabaseShutdown);
                        }
                        var msg = $"Successfully bulk inserted {count} documents in {sp.ElapsedMilliseconds:#,#;;0} ms";
                        if (Log.IsDebugEnabled)
                            Log.Debug(msg);
                        await _webSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, msg, CancellationToken.None);
                    }
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