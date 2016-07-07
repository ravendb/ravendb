using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron.Exceptions;

namespace Raven.Server.Documents.BulkInsert
{
    public unsafe class BulkInsertConnection : IDisposable
    {
        public static readonly byte[] ProcessingMessage = Encoding.UTF8.GetBytes("{'Type': 'Processing'}");
        public static readonly byte[] WaitingMessage = Encoding.UTF8.GetBytes("{'Type': 'Waiting'}");
        public static readonly byte[] CompletedMessage = Encoding.UTF8.GetBytes("{'Type': 'Completed'}");

        private readonly DocumentDatabase _database;
        private readonly JsonOperationContext _context;
        private readonly Stream _stream;

        private readonly Logger _logger;

        private class BulkInsertDoc
        {
            public UnmanagedBuffersPool.AllocatedMemoryData Memory;
            public byte* Pointer;
            public int Used;
        }

        private readonly BlockingCollection<BulkInsertDoc> _docsToWrite =
            new BlockingCollection<BulkInsertDoc>(512);

        private readonly BlockingCollection<byte[]> _messagesToClient =
            new BlockingCollection<byte[]>(32);

        private readonly BlockingCollection<BulkInsertDoc> _docsToRelease =
            new BlockingCollection<BulkInsertDoc>();

        private Task _replyToCustomer;
        private Task _insertDocuments;

        public BulkInsertConnection(DocumentDatabase database, JsonOperationContext context, Stream stream)
        {
            _database = database;
            _context = context;
            _stream = stream;
            _logger = database.LoggerSetup.GetLogger<BulkInsertConnection>(database.Name);
        }

        public void Execute()
        {
            _replyToCustomer = Task.Factory.StartNew(ReplyToClient);
            _insertDocuments = Task.Factory.StartNew(() =>
            {
                try
                {
                    InsertDocuments();
                }
                catch (Exception)
                {
                    _docsToRelease.Dispose(); // will abort the reading thread
                    throw;
                }
            });
            try
            {
                ReadBulkInsert();
                _insertDocuments.Wait(); // need to wait until this is completed
                _messagesToClient.Add(CompletedMessage);
            }
            catch (AggregateException e)
            {
                _docsToWrite.CompleteAdding();
                try
                {
                    _insertDocuments.Wait();
                }
                catch (Exception )
                {
                    // forcing observation of any potential errors
                }
                SendErrorToClient(e.InnerException);
            }
            catch (Exception e)
            {
                SendErrorToClient(e);
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
            catch (Exception )
            {
                // we don't care about any errors here, we just need to make sure that the thread
                // isn't sending stuff to the client while we are sending the error
            }
            try
            {
                var error = _context.ReadObject(new DynamicJsonValue
                {
                    ["Type"] = "Error",
                    ["Exception"] = e.ToString()
                }, "error/message");
                _context.Write(_stream, error);
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
                _stream.Write(bytes, 0, bytes.Length);
            }
        }

        private void InsertDocuments()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
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
                        while(_docsToWrite.TryTake(out doc, 500) == false)
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
            }
        }

        private void FlushDocuments(DocumentsOperationContext context, List<BulkInsertDoc> docsToWrite, ref int totalSize)
        {
            if (docsToWrite.Count == 0)
                return;

            // Three retries : 
            // 1st - if scratch buff full, 2nd - after asking for new flush, 3rd - waiting for the new flush to end
            int retry = 3;
            while (true)
            {
                try
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Writing {docsToWrite.Count:#,#} documents to disk using bulk insert, total {totalSize/1024:#,#} kb to write");
                    Stopwatch sp = Stopwatch.StartNew();
                    using (var tx = context.OpenWriteTransaction())
                    {
                        tx.InnerTransaction.LowLevelTransaction.IsLazyTransaction = true;

                        foreach (var bulkInsertDoc in docsToWrite)
                        {
                            var reader = new BlittableJsonReaderObject(bulkInsertDoc.Pointer, bulkInsertDoc.Used,
                                context);
                            reader.BlittableValidation();

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

                            _database.DocumentsStorage.Put(context, docKey, null, reader);

                        }
                        tx.Commit();
                    }
                    foreach (var bulkInsertDoc in docsToWrite)
                    {
                        _docsToRelease.Add(bulkInsertDoc);
                    }
                    docsToWrite.Clear();
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Writing {docsToWrite.Count:#,#} documents in bulk insert took {sp.ElapsedMilliseconds:#,#} ms");
                    return;
                }
                catch (ScratchBufferSizeLimitException e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Tried to lazy commit to finish scratch buffer size usage", e);
                    retry--;
                    if (retry == 0)
                        throw;
                    try
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            // this non lazy transaction forces the journal to actually
                            // flush everything
                            tx.Commit();
                        }
                        bool lockTaken = false;
                        var journal = _database.DocumentsStorage.Environment.Journal;
                        Debug.Assert(journal != null);
                        using (journal.Applicator.TryTakeFlushingLock(ref lockTaken))
                        {
                            if (lockTaken == false)
                            {
                                var sp = Stopwatch.StartNew();

                                // lets wait for the flush to end and retry put doc
                                using (journal.Applicator.TryTakeFlushingLock(ref lockTaken, TimeSpan.FromSeconds(30)))
                                {
                                    if (_logger.IsInfoEnabled)
                                    {
                                        _logger.Info($"Waiting for flush to complete for {sp.ElapsedMilliseconds:#,#} ms, flush completed: {lockTaken}");
                                    }
                                }
                            }
                            else
                            {
                                if (_logger.IsInfoEnabled)
                                {
                                    _logger.Info($"Forcing flush to data file to cleanup the scratch buffer in bulk insert");
                                }
                                // there's no flushing but scratch buffer full - let's flush and retry put doc
                                context.Environment().ForceLogFlushToDataFile(null, true);
                            }
                        }
                        // TODO : Measure IO times (RavenDB-4659) - ForceFlush on a retry
                    }
                    catch (TimeoutException)
                    {
                        // the flush thread is currently running... ?
                        if (_logger.IsInfoEnabled)
                            _logger.Info(
                                "Timed-out while trying to commit non lazy transaction on a retry to commit lazy transaction during scratch buffer flash. Will continue to retry");
                    }
                }
            }
        }

        private void ReadBulkInsert()
        {
            var managedBuffer = new byte[1024*32];
            fixed (byte* managedBufferPointer = managedBuffer)
            {
                while (true)
                {
                    var len = Read7BitEncodedInt();
                    if (len <= 0)
                    {
                        _docsToWrite.CompleteAdding();
                        break;
                    }

                    BulkInsertDoc buffer;
                    while (true)
                    {
                        bool hasFreeBuffer;
                        try
                        {
                            hasFreeBuffer = _docsToRelease.TryTake(out buffer);
                        }
                        catch (ObjectDisposedException)
                        {
                            // error during the insert, just quit and use the error handling to report to the user
                            return;
                        }
                        if (hasFreeBuffer == false)
                        {
                            var allocatedMemoryData = _context.GetMemory(len);
                            buffer = new BulkInsertDoc
                            {
                                Memory = allocatedMemoryData,
                                Pointer = (byte*)allocatedMemoryData.Address
                            };
                            break;
                        }
                        buffer.Used = 0;
                        if (buffer.Memory.SizeInBytes >= len)
                            break;
                        _context.ReturnMemory(buffer.Memory);
                    }
                    while (len > 0)
                    {
                        var read = _stream.Read(managedBuffer, 0, Math.Min(len, managedBuffer.Length));
                        if (read == 0)
                            throw new EndOfStreamException("Could not read expected document");
                        len -= read;

                        Memory.Copy(buffer.Pointer + buffer.Used, managedBufferPointer, read);

                        buffer.Used += read;
                    }
                    while (_docsToWrite.TryAdd(buffer, 500) == false)
                    {
                        _messagesToClient.Add(ProcessingMessage);
                    }
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
                int r = _stream.ReadByte();
                if (r == -1)
                    return -1;
                b = (byte)r;
                count |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return count;
        }

        public void Dispose()
        {
            _docsToRelease.Dispose();
            _docsToWrite.Dispose();
            _messagesToClient.Dispose();
        }
    }
}