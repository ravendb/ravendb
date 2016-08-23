using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using System.Text;
using Raven.Client.Replication.Messages;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class IncomingReplicationHandler : IDisposable
    {
        private readonly JsonOperationContext.MultiDocumentParser _multiDocumentParser;
        private readonly DocumentDatabase _database;
        private readonly TcpClient _tcpClient;
        private readonly NetworkStream _stream;
        private readonly DocumentsOperationContext _context;
        private Thread _incomingThread;
        private readonly CancellationTokenSource _cts;
        private readonly Logger _log;
        private readonly IDisposable _contextDisposable;

        public event Action<IncomingReplicationHandler, Exception> Failed;
        public event Action<IncomingReplicationHandler> DocumentsReceived;

        public IncomingReplicationHandler(JsonOperationContext.MultiDocumentParser multiDocumentParser, DocumentDatabase database, TcpClient tcpClient, NetworkStream stream, ReplicationLatestEtagRequest replicatedLastEtag)
        {
            ConnectionInfo = IncomingConnectionInfo.FromGetLatestEtag(replicatedLastEtag);
            _multiDocumentParser = multiDocumentParser;
            _database = database;
            _tcpClient = tcpClient;
            _stream = stream;
            _contextDisposable = _database.DocumentsStorage
                                          .ContextPool
                                          .AllocateOperationContext(out _context);

            _log = LoggerSetup.Instance.GetLogger<IncomingReplicationHandler>(_database.Name);
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }

        public void Start()
        {
            _incomingThread = new Thread(ReceiveReplicatedDocuments)
            {
                IsBackground = true,
                Name = $"Incoming replication {FromToString}"
            };
            _incomingThread.Start();
            if (_log.IsInfoEnabled)
                _log.Info($"Incoming replication thread started ({FromToString})");

        }

        long _prevRecievedEtag = -1;

        [ThreadStatic] public static bool IsIncomingReplicationThread;

        private void ReceiveReplicatedDocuments()
        {
            IsIncomingReplicationThread = true;

            bool exceptionLogged = false;
            try
            {
                using (_contextDisposable)
                using (_stream)
                using (var writer = new BlittableJsonTextWriter(_context, _stream))
                using (_multiDocumentParser)
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        _context.Reset();

                        using (var message = _multiDocumentParser.ParseToMemory("IncomingReplication/read-message"))
                        {
                            //note: at this point, the valid messages are heartbeat and replication batch.
                            _cts.Token.ThrowIfCancellationRequested();

                          try
                            {
                                ValidateReplicationBatchAndGetDocsCount(message);

                                long lastEtag;
                                if (message.TryGet("LastEtag", out lastEtag) == false)
                                    throw new InvalidDataException("The property 'LastEtag' wasn't found in replication batch, invalid data");

                                int replicatedDocsCount;
                                if (!message.TryGet("Documents", out replicatedDocsCount))
                                    throw new InvalidDataException($"Expected the 'Documents' field, but had no numeric field of this value, this is likely a bug");

                                ReceiveSingleBatch(replicatedDocsCount, lastEtag);

                                OnDocumentsReceived(this);

                                //return positive ack
                                SendStatusToSource(writer, lastEtag);
                            }
                            catch (Exception e)
                            {
                                //if we are disposing, ignore errors
                                if (!_cts.IsCancellationRequested && !(e is ObjectDisposedException))
                                {
                                    //return negative ack
                                    _context.Write(writer, new DynamicJsonValue
                                    {
                                        ["Type"] = ReplicationMessageReply.ReplyType.Error.ToString(),
                                        ["LastEtagAccepted"] = -1,
                                        ["Error"] = e.ToString()
                                    });

                                    exceptionLogged = true;

                                    if (_log.IsInfoEnabled)
                                        _log.Info($"Failed replicating documents from {FromToString}.",e);
                                    throw;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                //if we are disposing, do not notify about failure (not relevant)
                if (!_cts.IsCancellationRequested)
                {
                    if (_log.IsInfoEnabled && exceptionLogged == false)
                        _log.Info($"Connection error {FromToString}: an exception was thrown during receiving incoming document replication batch.",e );

                    OnFailed(e, this);
                }
            }
        }

        private unsafe void ReceiveSingleBatch(int replicatedDocsCount, long lastEtag)
        {
            var sw = Stopwatch.StartNew();
            using (var writeBuffer = _context.GetStream())
            {
                // this will read the documents to memory from the network
                // without holding the write tx open
                ReadDocumentsFromSource(writeBuffer, replicatedDocsCount);
                byte* buffer;
                int _;
                writeBuffer.EnsureSingleChunk(out buffer, out _);

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received {replicatedDocsCount:#,#;;0} documents to database in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                using (_context.OpenWriteTransaction())
                {
                    var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
                    foreach (var changeVectorEntry in _database.DocumentsStorage.GetDatabaseChangeVector(_context))
                    {
                        maxReceivedChangeVectorByDatabase[changeVectorEntry.DbId] = changeVectorEntry.Etag;
                    }

                    foreach (var doc in _replicatedDocs)
                    {
                        ReadChangeVector(doc, buffer, maxReceivedChangeVectorByDatabase);
						var json = new BlittableJsonReaderObject(
							buffer + doc.Position + (doc.ChangeVectorCount * sizeof(ChangeVectorEntry))
							, doc.DocumentSize, _context);

						switch (GetConflictStatus(_context, doc.Id, _tempReplicatedChangeVector))
	                    {
		                    case ConflictStatus.Update:
								_database.DocumentsStorage.Put(_context, doc.Id, null, json, _tempReplicatedChangeVector);
								break;
		                    case ConflictStatus.ShouldResolveConflict:
								_context.DocumentDatabase.DocumentsStorage.DeleteConflictsFor(_context, doc.Id);
								goto case ConflictStatus.Update;
							case ConflictStatus.Conflict:
								_database.DocumentsStorage.AddConflict(_context,doc.Id, json, _tempReplicatedChangeVector);
								break;
							case ConflictStatus.AlreadyMerged:
								//nothing to do
								break;
							default:
			                    throw new ArgumentOutOfRangeException("Invalid ConflictStatus");
	                    }
                    }
                    _database.DocumentsStorage.SetDatabaseChangeVector(_context,
                        maxReceivedChangeVectorByDatabase);

                    _database.DocumentsStorage.SetLastReplicateEtagFrom(_context, ConnectionInfo.SourceDatabaseId,
                        lastEtag);
                    _prevRecievedEtag = lastEtag;
                    _context.Transaction.Commit();
                }
                sw.Stop();

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Replication connection {FromToString}: received and written {replicatedDocsCount:#,#;;0} documents to database in {sw.ElapsedMilliseconds:#,#;;0} ms, with last etag = {lastEtag}.");
            }
        }

	    

        private unsafe void ReadChangeVector(ReplicationDocumentsPositions doc, byte* buffer,
            Dictionary<Guid, long> maxReceivedChangeVectorByDatabase)
        {
            if (_tempReplicatedChangeVector.Length != doc.ChangeVectorCount)
            {
                _tempReplicatedChangeVector = new ChangeVectorEntry[doc.ChangeVectorCount];
            }
            for (int i = 0; i < doc.ChangeVectorCount; i++)
            {
                _tempReplicatedChangeVector[i] = ((ChangeVectorEntry*) (buffer + doc.Position))[i];
                long etag;
                if (maxReceivedChangeVectorByDatabase.TryGetValue(_tempReplicatedChangeVector[i].DbId, out etag) == false ||
                    etag > _tempReplicatedChangeVector[i].Etag)
                {
                    maxReceivedChangeVectorByDatabase[_tempReplicatedChangeVector[i].DbId] = _tempReplicatedChangeVector[i].Etag;
                }
            }
        }

        private void SendStatusToSource(BlittableJsonTextWriter writer, long lastEtag)
        {
            var changeVector = new DynamicJsonArray();
            ChangeVectorEntry[] databaseChangeVector;

            using (_context.OpenReadTransaction())
            {
                databaseChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(_context);
            }

            foreach (var changeVectorEntry in databaseChangeVector)
            {
                changeVector.Add(new DynamicJsonValue
                {
                    ["DbId"] = changeVectorEntry.DbId.ToString(),
                    ["Etag"] = changeVectorEntry.Etag
                });
            }
            if (_log.IsInfoEnabled)
            {
                _log.Info(
                    $"Sending ok => {FromToString} with last etag {lastEtag} and change vector: {databaseChangeVector.Format()}");
            }
            _context.Write(writer, new DynamicJsonValue
            {
                ["Type"] = "Ok",
                ["LastEtagAccepted"] = lastEtag,
                ["Error"] = null,
                ["CurrentChangeVector"] = changeVector
            });

            writer.Flush();
        }

        private static void ValidateReplicationBatchAndGetDocsCount(BlittableJsonReaderObject message)
        {
            string messageType;
            if (!message.TryGet("Type", out messageType))
                throw new InvalidDataException("Expected the message to have a 'Type' field. The property was not found");

            if (!messageType.Equals("ReplicationBatch", StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"Expected the message 'Type = ReplicationBatch' field, but has 'Type={messageType}'. This is likely a bug.");

         
            
        }

        public string FromToString => $"from {ConnectionInfo.SourceDatabaseName} at {ConnectionInfo.SourceUrl} (into database {_database.Name})";
        public IncomingConnectionInfo ConnectionInfo { get; }

        private readonly byte[] _tempBuffer = new byte[32*1024];
        private ChangeVectorEntry[] _tempReplicatedChangeVector = new ChangeVectorEntry[0];
        private readonly List<ReplicationDocumentsPositions> _replicatedDocs = new List<ReplicationDocumentsPositions>(); 

        public struct ReplicationDocumentsPositions
        {
            public string Id;
            public int Position;
            public int ChangeVectorCount;
            public int DocumentSize;
        }

        private unsafe void ReadDocumentsFromSource(UnmanagedWriteBuffer writeBuffer, int replicatedDocs)
        {
            _replicatedDocs.Clear();

            fixed (byte* pTemp = _tempBuffer)
            {
                for (int x = 0; x < replicatedDocs; x++)
                {
                    var curDoc = new ReplicationDocumentsPositions
                    {
                        Position = writeBuffer.SizeInBytes
                    };
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    curDoc.ChangeVectorCount = *(int*) pTemp;
                   
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(ChangeVectorEntry) * curDoc.ChangeVectorCount);
                    writeBuffer.Write(_tempBuffer, 0, sizeof (ChangeVectorEntry)*curDoc.ChangeVectorCount);
                    
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var keySize = *(int*)pTemp;
                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, keySize);
                    curDoc.Id = Encoding.UTF8.GetString(_tempBuffer, 0, keySize);

                    _multiDocumentParser.ReadExactly(_tempBuffer, 0, sizeof(int));
                    var documentSize = curDoc.DocumentSize = *(int*)pTemp;
                    while (documentSize>0)
                    {
                        var read = _multiDocumentParser.Read(_tempBuffer, 0, Math.Min(_tempBuffer.Length, documentSize));
                        if(read == 0)
                            throw new EndOfStreamException();
                        writeBuffer.Write(pTemp, read);
                        documentSize -= read;
                    }
                    
                    _replicatedDocs.Add(curDoc);
                }
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            try
            {
                _stream.Dispose();
            }
            catch (Exception)
            {
            }
            try
            {
                _tcpClient.Dispose();
            }
            catch (Exception)
            {
            }

            if (_incomingThread != Thread.CurrentThread)
            {
                _incomingThread?.Join();
            }
            _incomingThread = null;
        }

        protected void OnFailed(Exception exception, IncomingReplicationHandler instance) => Failed?.Invoke(instance, exception);
        protected void OnDocumentsReceived(IncomingReplicationHandler instance) => DocumentsReceived?.Invoke(instance);

		private ConflictStatus GetConflictStatus(DocumentsOperationContext context, string key, ChangeVectorEntry[] remote)
		{
			//tombstones also can be a conflict entry
			var conflicts = context.DocumentDatabase.DocumentsStorage.GetConflictsFor(context, key);
			if (conflicts.Count > 0)
			{
				foreach (var existingConflict in conflicts)
				{
					if(GetConflictStatus(remote, existingConflict.ChangeVector) == ConflictStatus.Conflict)
						return ConflictStatus.Conflict;
				}

				return ConflictStatus.ShouldResolveConflict;
			}

			var result = context.DocumentDatabase.DocumentsStorage.GetDocumentOrTombstone(context, key);
			ChangeVectorEntry[] local;

			if (result.Item1 != null)
				local = result.Item1.ChangeVector;
			else if (result.Item2 != null)
				local = result.Item2.ChangeVector;
			else return ConflictStatus.Update; //document with 'key' doesnt exist locally, so just do PUT

			return GetConflictStatus(remote, local);
		}

		public enum ConflictStatus
		{
			Update,
			Conflict,
			AlreadyMerged,
			ShouldResolveConflict
		}

		public static ConflictStatus GetConflictStatus(ChangeVectorEntry[] remote, ChangeVectorEntry[] local)
	    {
			//any missing entries from a change vector are assumed to have zero value
			var remoteHasLargerEntries = local.Length < remote.Length; 
		    var localHasLargerEntries = remote.Length < local.Length;

		    int remoteEntriesTakenIntoAccount = 0;
		    for (int index = 0; index < local.Length; index++)
		    {
			    if (remote.Length < index && remote[index].DbId == local[index].DbId)
			    {
				    remoteHasLargerEntries |= remote[index].Etag > local[index].Etag;
					localHasLargerEntries |= local[index].Etag > remote[index].Etag;
					remoteEntriesTakenIntoAccount++;
				}
			    else
			    {
				    var updated = false;
				    for (var remoteIndex = 0; remoteIndex < remote.Length; remoteIndex++)
				    {
					    if (remote[remoteIndex].DbId == local[index].DbId)
					    {
							remoteHasLargerEntries |= remote[remoteIndex].Etag > local[index].Etag;
							localHasLargerEntries |= local[index].Etag > remote[remoteIndex].Etag;
						    remoteEntriesTakenIntoAccount++;
							updated = true;
						}
					}

				    if (!updated)
					    localHasLargerEntries = true;
			    }
		    }
			remoteHasLargerEntries |= remoteEntriesTakenIntoAccount < remote.Length;

			if (remoteHasLargerEntries && localHasLargerEntries)
				return ConflictStatus.Conflict;

		    if (!remoteHasLargerEntries && localHasLargerEntries)
			    return ConflictStatus.AlreadyMerged;

		    return ConflictStatus.Update;
	    }
    }
}
