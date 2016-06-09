// -----------------------------------------------------------------------
//  <copyright file="BulkInsertHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Exceptions;
using Bits = Sparrow.Binary.Bits;

namespace Raven.Server.Documents.Handlers
{
    public class BulkInsertHandler : DatabaseRequestHandler
    {
        private const int NumberOfBuffersUsed = 8;
        private readonly BlockingCollection<BulkBufferInfo> _fullBuffers = new BlockingCollection<BulkBufferInfo>();
        private readonly BlockingCollection<BulkBufferInfo> _freeBuffers = new BlockingCollection<BulkBufferInfo>();

        public static readonly ArraySegment<byte> ProcessingMessage = new ArraySegment<byte>(Encoding.UTF8.GetBytes("{'Type': 'Processing'}"));
        public static readonly ArraySegment<byte> WaitingMessage = new ArraySegment<byte>(Encoding.UTF8.GetBytes("{'Type': 'Waiting'}"));
        public static readonly ArraySegment<byte> CompletedMessage = new ArraySegment<byte>(Encoding.UTF8.GetBytes("{'Type': 'Completed'}"));
        public static byte[] ProcessedMessageArray = new byte[1024];
        public static readonly ArraySegment<byte> ProcessedMessage = new ArraySegment<byte>(ProcessedMessageArray);

        private WebSocket _webSocket;
        private static readonly object SendLock = new object();
        private static readonly object LockRecvAsync = new object();



        private async Task SendCloseMessageToClient(WebSocketCloseStatus status, string msg)
        {
            await _webSocket.CloseOutputAsync(status, msg, Database.DatabaseShutdown);

        }

        private void SendMessageToClient(ArraySegment<byte> msg)
        {
            lock (SendLock)
            {
                _webSocket.SendAsync(msg, WebSocketMessageType.Text, true, Database.DatabaseShutdown).Wait();
            }
        }

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

        public unsafe void InsertDocuments()
        {
            long processedAccumulator = 0;
            try
            {
                DocumentsOperationContext context;

                using (ContextPool.AllocateOperationContext(out context))
                {
                    StorageEnvironment env = context.Environment();

                    while (_fullBuffers.IsCompleted == false)
                    {
                        BulkBufferInfo current;
                        try
                        {
                            int retry = 60;
                            while (_fullBuffers.TryTake(out current, 500, Database.DatabaseShutdown) == false)
                            {
                                if (--retry == 0)
                                {
                                    throw new InvalidOperationException("Server waited " + (500 * 60) / 1000 + " Seconds, but the client didn't send any documents or completion message");
                                }
                                SendMessageToClient(WaitingMessage);
                            }
                        }
                        catch (InvalidOperationException) // adding completed
                        {
                            break;
                        }
                        if (Log.IsDebugEnabled)
                            Log.Debug($"Starting bulk insert batch with size {current.Used:#,#;;0} bytes");
                        int count = 0;
                        var sp = Stopwatch.StartNew();
                        const int retries = 6;
                        for (int i = 0; i < retries; i++)
                        {
                            try
                            {
                                using (var tx = context.OpenWriteTransaction())
                                {
                                    tx.InnerTransaction.LowLevelTransaction.IsLazyTransaction = true;

                                    byte* docPtr = (byte*)current.Buffer.Address;
                                    var end = docPtr + current.Used;
                                    while (docPtr < end)
                                    {
                                        count++;
                                        var size = *(int*)docPtr;
                                        docPtr += sizeof(int);
                                        if (size + docPtr > end) //TODO: Better error
                                            throw new InvalidDataException(
                                                "The blittable size specified is more than the available data, aborting...");

                                        //TODO: Paranoid mode, has to validate the data is safe
                                        var reader = new BlittableJsonReaderObject(docPtr, size, context);
                                        docPtr += size;

                                        string docKey;
                                        BlittableJsonReaderObject metadata;
                                        if (reader.TryGet(Constants.Metadata, out metadata) == false)
                                        {
                                            const string message = "'@metadata' is missing in received document for bulk insert";
                                            throw new InvalidDataException(message);
                                        }
                                        if (metadata.TryGet(Constants.MetadataDocId, out docKey) == false)
                                        {
                                            const string message = "'@id' is missing in received document for bulk insert";
                                            throw new InvalidDataException(message);
                                        }

                                        Database.DocumentsStorage.Put(context, docKey, null, reader);
                                    }
                                    _freeBuffers.Add(current);
                                    tx.Commit();
                                }
                                break;
                            }
                            catch (ScratchBufferSizeLimitException e)
                            {
                                int timeToWait = 1000;
                                if (i == retries - 1)
                                {
                                    Log.ErrorException(
                                        $"Tried to lazy commit {retries} times but didn't succeed to finish scratch buffer flushing for at least {timeToWait * (retries - 1)} mSec",
                                        e);
                                    throw;
                                }
                                try
                                {
                                    using (var tx = context.OpenWriteTransaction())
                                    {
                                        // this non lazy transaction forces the journal to actually
                                        // flush everything
                                        tx.Commit();
                                    }
                                    env.ForceLogFlushToDataFile(null, true);
                                    // TODO : Measure IO times (RavenDB-4659) - ForceFlush on a retry
                                }
                                catch (TimeoutException)
                                {
                                    // the flush thread is currently running... ?
                                    Log.Warn(
                                        "Timed-out while trying to commit non lazy transaction on a retry to commit lazy transaction during scratch buffer flash. Will continue to retry");
                                }
                            }
                        }
                        if (Log.IsDebugEnabled)
                            Log.Debug(
                                $"Completed bulk insert batch with {count} documents in {sp.ElapsedMilliseconds:#,#;;0} ms");

                        processedAccumulator += current.Used;

                        var proccessedString = "{'Type': 'Processed', 'Size': " + processedAccumulator + "}";

                        Encoding.UTF8.GetBytes(proccessedString, 0, proccessedString.Length, ProcessedMessageArray, 0);
                        SendMessageToClient(ProcessedMessage);
                    }

                    using (var tx = context.OpenWriteTransaction())
                    {
                        // this non lazy transaction forces the journal to actually
                        // flush everything
                        tx.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                Log.ErrorException("Server reported error during bulk insert", ex);
                _fullBuffers.CompleteAdding();
                throw; // TODO: check this throwing is handled propertly later on
            }
        }

        public BlittableJsonReaderObject TryReadFromBuffer(
                                                        JsonOperationContext context,
                                                        ArraySegment<byte> buffer,
                                                        int bufferSize,
                                                        string debugTag)
        {
            var jsonParserState = new JsonParserState();
            using (var parser = new UnmanagedJsonParser(context, jsonParserState, debugTag))
            {
                var writer = new BlittableJsonDocumentBuilder(context,
                    BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState);

                writer.ReadObject();

                parser.SetBuffer(buffer.Array, bufferSize);
                if (writer.Read() == false)
                    return null;

                writer.FinalizeDocument();
                return writer.CreateReader();
            }
        }

        [RavenAction("/databases/*/bulkInsert", "GET", "/databases/{databaseName:string}/bulkInsert")]
        public async Task BulkInsert()
        {
            DocumentsOperationContext context;
            using (_webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false))
            using (ContextPool.AllocateOperationContext(out context))
            {


                //var buffer = new ArraySegment<byte>(context.GetManagedBuffer());
                //WebSocketReceiveResult result;
                //while (true)
                //{
                //    try
                //    {
                //         result =await _webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
                //        //result.EndOfMessage
                //        if (result.CloseStatus != null)
                //        {
                //            Console.WriteLine("Closing on " + result.CloseStatus + "," + result.CloseStatusDescription);
                //            try
                //            {
                //                await SendCloseMessageToClient(WebSocketCloseStatus.NormalClosure, "normal closer");
                //            }
                //            catch (Exception eee)
                //            {
                //                Console.WriteLine("HERE " + eee);
                //            }
                //            break;
                //        }
                //    }
                //    catch (Exception exx)
                //    {
                //        Console.WriteLine("ERRR222:" + exx);
                //        await SendCloseMessageToClient(WebSocketCloseStatus.InternalServerError, "{'Type': 'Error', 'Exception': 'TEST'}");
                //        throw;
                //    }
                //}








                Log.Debug("Starting bulk insert operation");

                for (int i = 0; i < NumberOfBuffersUsed; i++)
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
                    using (var stream = context.GetStream())
                    {
                        var current = _freeBuffers.Take();
                        int count = 0;
                        var sp = Stopwatch.StartNew();
                        while (true)
                        {
                            // var result = await _webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown).ConfigureAwait(false);

                            WebSocketReceiveResult result;
                            lock (LockRecvAsync)
                            {
                                try
                                {
                                    var recvTask = _webSocket.ReceiveAsync(buffer, Database.DatabaseShutdown);
                                    recvTask.Wait();
                                    if (recvTask.IsFaulted)
                                    {
                                        Console.WriteLine("ERRR:" + recvTask.Exception);
                                    }
                                    result = recvTask.Result;
                                }
                                catch (Exception exx)
                                {
                                    Console.WriteLine("ERRR222:" + exx);
                                    throw;
                                }
                            }

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

                                while (_freeBuffers.TryTake(out current, 1000) == false)
                                {
                                    SendMessageToClient(ProcessingMessage);
                                }

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
                            var res = await Task.WhenAny(task, Task.Delay(TimeSpan.FromMilliseconds(1000)));
                            if (res == task)
                                break;

                            SendMessageToClient(ProcessingMessage);
                        }
                        var msg =
                                $"Successfully bulk inserted {count} documents in {sp.ElapsedMilliseconds:#,#;;0} ms";
                        if (Log.IsDebugEnabled)
                            Log.Debug(msg);

                        SendMessageToClient(CompletedMessage);

                        SendCloseMessageToClient(WebSocketCloseStatus.NormalClosure, msg);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("ERRO: " + e);
                    // TODO :: check why -  "System.InvalidOperationException: Unexpected reserved bits set"
                    _fullBuffers.CompleteAdding();
                    try
                    {
                        SendCloseMessageToClient(WebSocketCloseStatus.InternalServerError, "{'Type': 'Error', 'Exception': '" + e.ToString().Replace("'", "\\'") + "'}");
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