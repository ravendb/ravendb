using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.TcpHandlers
{
    public unsafe class BulkInsertConnection : IDisposable
    {
        public static readonly byte[] ProcessingMessage = Encoding.UTF8.GetBytes("{'Type': 'Processing'}");
        public static readonly byte[] WaitingMessage = Encoding.UTF8.GetBytes("{'Type': 'Waiting'}");
        public static readonly byte[] CompletedMessage = Encoding.UTF8.GetBytes("{'Type': 'Completed'}");

        public readonly TcpConnectionOptions TcpConnection;
        private readonly Logger _logger;

        private class BulkInsertDoc
        {
            public AllocatedMemoryData Memory;
            public byte* Pointer;
            public int Used;
        }

        private readonly BlockingCollection<BulkInsertDoc> _docsToWrite =
            new BlockingCollection<BulkInsertDoc>(512);

        private readonly BlockingCollection<byte[]> _messagesToClient =
            new BlockingCollection<byte[]>(32);

        private readonly UnmanagedBuffersPool _memPool;

        private Task _replyToCustomer;
        private Task _insertDocuments;

        public BulkInsertConnection(TcpConnectionOptions tcpConnection, Logger logger)
        {
            TcpConnection = tcpConnection;
            _logger = logger;
            _memPool = new UnmanagedBuffersPool("bulk-insert",tcpConnection.DocumentDatabase.Name);
        }

        public void Execute()
        {
            _replyToCustomer = Task.Factory.StartNew(ReplyToClient);
            _insertDocuments = Task.Factory.StartNew(InsertDocuments);
            try
            {
                ReadBulkInsert();
                _insertDocuments.Wait(); // need to wait until this is completed
                _messagesToClient.Add(CompletedMessage);
                _messagesToClient.CompleteAdding();
                _replyToCustomer.Wait();
                TcpConnection.Stream.Flush(); // make sure that everyting goes to the client
            }
            catch (AggregateException e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error occured while read bulk insert operation", e.InnerException);
                WaitForBackgroundTasks();
                SendErrorToClient(e.InnerException);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Server internal error while in bulk insert process", e);
                WaitForBackgroundTasks();
                SendErrorToClient(e);
            }
        }

        private void WaitForBackgroundTasks()
        {
            _docsToWrite.CompleteAdding();
            _messagesToClient.CompleteAdding();

            try
            {
                _insertDocuments.Wait();
            }
            catch (Exception insertDocumentsException)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error in server while inserting bulk documents", insertDocumentsException);
                // forcing observation of any potential errors
            }
            try
            {
                _replyToCustomer.Wait();
            }
            catch (Exception replyToClientException)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Couldn't reply to client with server's error in bulk insert (maybe client was disconnected)",
                        replyToClientException);
                // forcing observation of any potential errors
            }
        }

        private void SendErrorToClient(Exception e)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info("Failure during bulk insert", e);

            _messagesToClient.CompleteAdding();
            try
            {
                _replyToCustomer.Wait();
            }
            catch (Exception)
            {
                // we don't care about any errors here, we just need to make sure that the thread
                // isn't sending stuff to the client while we are sending the error
            }
            try
            {
                JsonOperationContext context;
                using (TcpConnection.ContextPool.AllocateOperationContext(out context))
                {
                    var error = context.ReadObject(new DynamicJsonValue
                    {
                        ["Type"] = "Error",
                        ["Exception"] = e.ToString()
                    }, "error/message");

                    using (var countingStream = new CountingStream(TcpConnection.Stream))
                    {
                        context.Write(countingStream, error);
                        TcpConnection.RegisterBytesSent(countingStream.NumberOfWrittenBytes);
                    }
                }

                  
            }
            catch (Exception errorSending)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not write bulk insert error to client", errorSending);
            }
        }

        private void ReplyToClient()
        {
            while (_messagesToClient.IsCompleted == false)
            {
                byte[] bytes;
                try
                {
                    bytes = _messagesToClient.Take();
                }
                catch (InvalidOperationException)
                {
                    return;
                }
                TcpConnection.Stream.Write(bytes, 0, bytes.Length);
                TcpConnection.RegisterBytesSent(bytes.Length);
            }
        }

        private void InsertDocuments()
        {
            DocumentsOperationContext context;

            using (TcpConnection.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                int totalSize = 0;
                var docsToWrite = new List<BulkInsertDoc>();
                while (_docsToWrite.IsCompleted == false)
                {
                    BulkInsertDoc doc;
                    if (_docsToWrite.TryTake(out doc) == false)
                    {
                        if (docsToWrite.Count != 0)
                        {
                            FlushDocuments(context, docsToWrite, ref totalSize);
                            continue;
                        }

                        var retry = 60;
                        while (_docsToWrite.TryTake(out doc, 500) == false)
                        {
                            if (_docsToWrite.IsCompleted == false)
                                break;

                            if (--retry == 0)
                            {
                                var message = "Server waited " + (500 * 60 / 1000) + " seconds, but the client didn't send any documents or completion message";
                                if (_logger.IsInfoEnabled)
                                    _logger.Info(message);
                                throw new InvalidOperationException(message);
                            }
                            _messagesToClient.Add(WaitingMessage);
                        }
                    }
                    if (doc != null)
                    {
                        totalSize += doc.Used;
                        docsToWrite.Add(doc);
                        if (totalSize > 1024 * 1024 * 16)
                        {
                            FlushDocuments(context, docsToWrite, ref totalSize);
                        }
                    }
                }
                FlushDocuments(context, docsToWrite, ref totalSize);
                using (var tx = context.OpenWriteTransaction())
                {
                    // this empty tx forces the journal to flush all pending lazy transactions
                    tx.Commit();
                }
            }
        }

        private void FlushDocuments(DocumentsOperationContext context, List<BulkInsertDoc> docsToWrite, ref int totalSize)
        {
            if (docsToWrite.Count == 0)
                return;
            if (_logger.IsInfoEnabled)
                _logger.Info(
                    $"Writing {docsToWrite.Count:#,#} documents to disk using bulk insert, total {totalSize/1024:#,#} kb to write");
            Stopwatch sp = Stopwatch.StartNew();
            using (var tx = context.OpenWriteTransaction())
            {
                tx.InnerTransaction.LowLevelTransaction.IsLazyTransaction = true;

                foreach (var bulkInsertDoc in docsToWrite)
                {
                    var reader = new BlittableJsonReaderObject(bulkInsertDoc.Pointer, bulkInsertDoc.Used, context);
                    reader.BlittableValidation();

                    string docKey;
                    BlittableJsonReaderObject metadata;
                    if (reader.TryGet(Constants.Documents.Metadata.Key, out metadata) == false)
                    {
                        const string message = "'@metadata' is missing in received document for bulk insert";
                        throw new InvalidDataException(message);
                    }
                    if (metadata.TryGet(Constants.Documents.Metadata.Id, out docKey) == false)
                    {
                        const string message = "'@id' is missing in received document for bulk insert";
                        throw new InvalidDataException(message);
                    }

                    TcpConnection.DocumentDatabase.DocumentsStorage.Put(context, docKey, null, reader);
                }
                tx.Commit();
            }
            foreach (var bulkInsertDoc in docsToWrite)
            {
                _memPool.Return(bulkInsertDoc.Memory);
            }
            if (_logger.IsInfoEnabled)
                _logger.Info(
                    $"Writing {docsToWrite.Count:#,#} documents in bulk insert took {sp.ElapsedMilliseconds:#,#l;0} ms");
            docsToWrite.Clear();
            totalSize = 0;
        }

        private void ReadBulkInsert()
        {
            while (true)
            {
                // _context.Reset(); - we cannot reset the context here
                // because the memory is being used by the other threads 
                // we avoid the memory leak of infinite usage by limiting 
                // the number of buffers we get from the context and then
                // reusing them
                var len = Read7BitEncodedInt();
                if (len <= 0)
                {
                    _docsToWrite.CompleteAdding();
                    break;
                }

                var buffer = new BulkInsertDoc
                {
                    Memory = _memPool.Allocate(len),
                };
                buffer.Pointer = buffer.Memory.Address;
                buffer.Used = 0;

                while (len > 0)
                {
                    if (TcpConnection.PinnedBuffer.Valid == TcpConnection.PinnedBuffer.Used)
                    {
                        var read = TcpConnection.Stream.Read(TcpConnection.PinnedBuffer.Buffer.Array,
                            TcpConnection.PinnedBuffer.Buffer.Offset, TcpConnection.PinnedBuffer.Buffer.Count);
                        TcpConnection.RegisterBytesReceived(read);
                        if (read == 0)
                            throw new EndOfStreamException("Could not read expected document");
                        TcpConnection.PinnedBuffer.Valid = read;
                        TcpConnection.PinnedBuffer.Used = 0;
                    }

                    var min = Math.Min(len, TcpConnection.PinnedBuffer.Valid - TcpConnection.PinnedBuffer.Used);
                    Memory.Copy(buffer.Pointer + buffer.Used, TcpConnection.PinnedBuffer.Pointer + TcpConnection.PinnedBuffer.Used, min);
                    TcpConnection.PinnedBuffer.Used += min;
                    len -= min;
                    buffer.Used += min;
                }
                while (_docsToWrite.TryAdd(buffer, 500) == false)
                {
                    _messagesToClient.Add(ProcessingMessage);
                }
            }
        }

        private int Read7BitEncodedInt()
        {
            // Read out an Int32 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            // we assume that the value shouldn't be zero very often
            // because then we'll always take 5 bytes to store it
            int count = 0;
            int shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    throw new FormatException("Bad variable size int");
                if (TcpConnection.PinnedBuffer.Valid == TcpConnection.PinnedBuffer.Used)
                {
                    var read = TcpConnection.Stream.Read(TcpConnection.PinnedBuffer.Buffer.Array,
                        TcpConnection.PinnedBuffer.Buffer.Offset, TcpConnection.PinnedBuffer.Buffer.Count);
                    TcpConnection.RegisterBytesReceived(read);
                    if (read == 0)
                        return -1;
                    TcpConnection.PinnedBuffer.Valid = read;
                    TcpConnection.PinnedBuffer.Used = 0;
                }
                b = TcpConnection.PinnedBuffer.Pointer[TcpConnection.PinnedBuffer.Used++];
                count |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return count;
        }

        public void Dispose()
        {
            //dispose those too if we dispose before finishing the bulk insert
            //(for example if we finish early because of an exception)
            foreach (var bulkInsertDoc in _docsToWrite)
            {
                _memPool.Return(bulkInsertDoc.Memory);
            }
            _memPool.Dispose();
            _docsToWrite.Dispose();
            _messagesToClient.Dispose();
            try
            {
                TcpConnection.Dispose();
            }
            catch (Exception)
            {
            }
        }

        public static void Run(TcpConnectionOptions tcpConnectionOptions)
        {
            var bulkInsertThread = new Thread(() =>
            {

                var logger = LoggingSource.Instance.GetLogger<BulkInsertConnection>(tcpConnectionOptions.DocumentDatabase.Name);
                try
                {
                    using (var bulkInsert = new BulkInsertConnection(tcpConnectionOptions, logger))
                    using(tcpConnectionOptions.ConnectionProcessingInProgress())
                    {
                        bulkInsert.Execute();
                    }
                }
                catch (Exception e)
                {
                    if (logger.IsInfoEnabled)
                    {
                        logger.Info("Failed to process bulk insert run", e);
                    }
                    try
                    {
                        JsonOperationContext context;
                        using(tcpConnectionOptions.ContextPool.AllocateOperationContext(out context))
                        using (var writer = new BlittableJsonTextWriter(context, tcpConnectionOptions.Stream))
                        {
                            context.Write(writer, new DynamicJsonValue
                            {
                                ["Type"] = "Error",
                                ["Exception"] = e.ToString()
                            });
                        }
                    }
                    catch (Exception)
                    {
                    }
                }
                finally
                {
                    tcpConnectionOptions.Dispose();

                    // Thread is going to die, let us release those resources early, instead of waiting for finalizer
                    ByteStringMemoryCache.Clean();
                    tcpConnectionOptions.DocumentDatabase.DocumentsStorage.ContextPool.Clean();
                }
            })
            {
                IsBackground = true,
                Name = "Bulk Insert Operation"
            };
            bulkInsertThread.Start();
        }
    }
}