using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Client.Replication.Messages;
using Raven.Server.Extensions;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationIndexTransformerSender
    {
        private readonly Logger _log;
        private long _lastEtag;
        private readonly SortedList<long, ReplicationBatchIndexItem> _orderedReplicaItems;
        private readonly Stream _stream;
        private readonly OutgoingReplicationHandler _parent;
        private readonly byte[] _tempBuffer = new byte[512 * 1024];

        public ReplicationIndexTransformerSender(Stream stream, OutgoingReplicationHandler parent, Logger log)
        {
            _log = log;
            _orderedReplicaItems = new SortedList<long, ReplicationBatchIndexItem>();
            _stream = stream;
            _parent = parent;
        }

        public bool ExecuteReplicationOnce()
        {
            _orderedReplicaItems.Clear();
            var readTx = _parent._context.OpenReadTransaction();
            try
            {
                var sp = Stopwatch.StartNew();
                var timeout = Debugger.IsAttached ? 60 * 1000 : 1000;
                while (sp.ElapsedMilliseconds < timeout)
                {
                    _lastEtag = _parent._lastSentIndexOrTransformerEtag;

                    _parent.CancellationToken.ThrowIfCancellationRequested();

                    var indexAndTransformerMetadata = _parent._database.IndexMetadataPersistence.GetAfter(_lastEtag + 1, 0, 1024);
                    using (var stream = new MemoryStream())
                    {
                        foreach (var item in indexAndTransformerMetadata)
                        {
                            _parent.CancellationToken.ThrowIfCancellationRequested();
                            stream.Position = 0;
                            using (var writer = new BlittableJsonTextWriter(_parent._context, stream))
                            {
                                switch (item.Type)
                                {
                                    case IndexesEtagsStorage.IndexEntryType.Index:
                                        var index = _parent._database.IndexStore.GetIndex(item.Id);
                                        if(index == null) //precaution
                                            throw new InvalidDataException($"Index with name {item.Name} has metadata, but is not at the index store. This is not supposed to happen and is likely a bug.");

                                        IndexProcessor.Export(writer, index, _parent._context, false);
                                        break;
                                    case IndexesEtagsStorage.IndexEntryType.Transformer:
                                        var transformer = _parent._database.TransformerStore.GetTransformer(item.Id);
                                        if (transformer == null) //precaution
                                            throw new InvalidDataException($"Transformer with name {item.Name} has metadata, but is not at the transformer store. This is not supposed to happen and is likely a bug.");

                                        TransformerProcessor.Export(writer, transformer, _parent._context);
                                        break;
                                    default:
                                        throw new ArgumentOutOfRangeException(nameof(item),
                                            "Unexpected item type in index/transformer metadata. This is not supposed to happen.");
                                }

                                writer.Flush();

                                stream.Position = 0;
                                AddReplicationItemToBatch(new ReplicationBatchIndexItem
                                {
                                    Name = item.Name,
                                    ChangeVector = item.ChangeVector,
                                    Etag = item.Etag,
                                    Type = (int) item.Type,
                                    Definition = _parent._context.ReadForMemory(stream,"Index/Transformer Replication - Reading definition into memory")
                                });
                            }
                        }
                    }
                    // if we are at the end, we are done
                    if (_lastEtag <= _parent._database.IndexMetadataPersistence.ReadLastEtag(_parent._context.Transaction.InnerTransaction))
                    {
                        break;
                    }
                }

                if (_log.IsInfoEnabled)
                {
                    _log.Info(
                        $"Found {_orderedReplicaItems.Count:#,#;;0} indexes/transformers to replicate to {_parent.Destination.Database} @ {_parent.Destination.Url} in {sp.ElapsedMilliseconds:#,#;;0} ms.");
                }

                if (_orderedReplicaItems.Count == 0)
                {
                    var hasModification = _lastEtag != _parent._lastSentIndexOrTransformerEtag;
                    _parent._lastSentIndexOrTransformerEtag = _lastEtag;
                    // ensure that the other server is aware that we skipped 
                    // on (potentially a lot of) documents to send, and we update
                    // the last etag they have from us on the other side
                    _parent.SendHeartbeat();
                    return hasModification;
                }

                _parent.CancellationToken.ThrowIfCancellationRequested();

                SendIndexTransformerBatch();

                return true;
            }
            finally
            {
                //release memory at the end of the operation
                foreach (var item in _orderedReplicaItems)
                    item.Value.Definition.Dispose();

                _orderedReplicaItems.Clear();

                if (readTx.Disposed == false)
                    readTx.Dispose();
            }
        }

        private void AddReplicationItemToBatch(ReplicationBatchIndexItem item)
        {          
            // destination already has it
            if (item.ChangeVector.GreaterThan(_parent._destinationLastKnownDocumentChangeVector) == false)
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info(
                        $"Skipping replication of {item.Name} because destination has a higher change vector. Source: {item.ChangeVector.Format()} < Dest: {_parent._destinationLastKnownDocumentChangeVectorAsString} ");
                }
                return;
            }
            _lastEtag = Math.Max(_lastEtag, item.Etag);
            _orderedReplicaItems.Add(item.Etag, item);
        }

        private void SendIndexTransformerBatch()
        {
            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Starting sending replication batch ({_parent._database.Name}) with {_orderedReplicaItems.Count:#,#;;0} indexes/transformers, and last etag {_lastEtag}");

            var sw = Stopwatch.StartNew();
            var headerJson = new DynamicJsonValue
            {
                ["Type"] = "ReplicationBatch",
                ["LastIndexOrTransformerEtag"] = _lastEtag,
                ["IndexesAndTransformers"] = _orderedReplicaItems.Count
            };
            _parent.WriteToServerAndFlush(headerJson);

            foreach (var item in _orderedReplicaItems)
                WriteMetadataToServer(item.Value);

            // we can release the read transaction while we are waiting for 
            // reply from the server and not hold it for a long time
            _parent._context.Transaction.Dispose();

            _stream.Flush();
            sw.Stop();

            _parent._lastSentIndexOrTransformerEtag = _lastEtag;

            if (_log.IsInfoEnabled && _orderedReplicaItems.Count > 0)
                _log.Info(
                    $"Finished sending replication batch. Sent {_orderedReplicaItems.Count:#,#;;0} documents in {sw.ElapsedMilliseconds:#,#;;0} ms. First sent etag = {_orderedReplicaItems[0].Etag}, last sent etag = {_lastEtag}");

            _parent._lastIndexOrTransformerSentTime = DateTime.UtcNow;
            using (_parent._context.OpenReadTransaction())
            {
                _parent.HandleServerResponse();
            }
        }

        private unsafe void WriteMetadataToServer(ReplicationBatchIndexItem item)
        {
            var changeVectorSize = item.ChangeVector.Length * sizeof(ChangeVectorEntry);
            var sizeOfNameInBytes = Encoding.UTF8.GetMaxByteCount(item.Name.Length);
            var requiredSize = changeVectorSize +
                               sizeof(int) + // # of change vector entries
                               sizeof(int) + // size of type
                               sizeof(long) + //size of etag
                               sizeOfNameInBytes +
                               item.Definition.Size +
                               sizeof(int); // size of definition
            
            if (requiredSize > _tempBuffer.Length)
                ThrowTooManyChangeVectorEntries(item.Name, item.ChangeVector, requiredSize);

            fixed (byte* pTemp = _tempBuffer)
            {
                int tempBufferPos = 0;
                //start writing change vector
                fixed (ChangeVectorEntry* pChangeVectorEntries = item.ChangeVector)
                {
                    *(int*) pTemp = item.ChangeVector.Length; //write change vector length
                    tempBufferPos += sizeof(int);

                    //write change vector entries
                    Memory.Copy(pTemp + tempBufferPos, (byte*) pChangeVectorEntries, changeVectorSize);
                    tempBufferPos += changeVectorSize;
                }
                //end writing change vector

                *(long*) (pTemp + tempBufferPos) = item.Etag; //write etag
                tempBufferPos += sizeof(long);

                //start writing index/transformer metadata name
                *(int*) (pTemp + tempBufferPos) = sizeOfNameInBytes; //write the size of the name string
                tempBufferPos += sizeof(int);

                //start writing the name string characters
                fixed (char* pName = item.Name)
                {
                    var destBuffer = (char*) pTemp;
                    int src = 0;
                    for (int dest = tempBufferPos; dest < item.Name.Length + tempBufferPos; dest++)
                        destBuffer[dest] = pName[src++];
                }
                tempBufferPos += sizeOfNameInBytes;
                //end writing the name string characters
                //end writing index/transformer metadata name

                //start writing index/transformer definition                
                //first write index/transformer definition size
                *(int*) (pTemp + tempBufferPos) = item.Definition.Size;
                tempBufferPos += sizeof(int);

                //start writing definition json data
                var docReadPos = 0;
                while (docReadPos < item.Definition.Size)
                {
                    var sizeToCopy = Math.Min(item.Definition.Size - docReadPos, _tempBuffer.Length - tempBufferPos);
                    if (sizeToCopy == 0) // buffer is full, need to flush it
                    {
                        _stream.Write(_tempBuffer, 0, tempBufferPos);
                        tempBufferPos = 0;
                        continue;
                    }
                    Memory.Copy(pTemp + tempBufferPos, item.Definition.BasePointer + docReadPos, sizeToCopy);
                    tempBufferPos += sizeToCopy;
                    docReadPos += sizeToCopy;
                }
                //end writing definition json data
                //end writing index/transformer definition
                _stream.Write(_tempBuffer, 0, tempBufferPos);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowTooManyChangeVectorEntries(string name, ChangeVectorEntry[] changeVector, int requiredSize)
        {
            throw new ArgumentOutOfRangeException("index/transformer change vector",
                $"Index/Transformer with name = '{name}' size is too large to replicate. Probably has too many change vector entries (change vector size = {changeVector.Length}, replicated index size = {requiredSize})");
        }

    }
}