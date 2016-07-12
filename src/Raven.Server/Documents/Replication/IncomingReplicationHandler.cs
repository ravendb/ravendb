using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class IncomingReplicationHandler : IDisposable
    {
	    private readonly IncomingConnectionInfo _connectionInfo;
	    private readonly TcpConnectionHeaderMessage _incomingMessageHeader;
	    private readonly JsonOperationContext.MultiDocumentParser _multiDocumentParser;
	    private readonly DocumentDatabase _database;
	    private NetworkStream _stream;
	    private DocumentsOperationContext _context;
	    private Thread _incomingThread;
	    private readonly CancellationTokenSource _cts;
	    private readonly Logger _log;
	    private readonly BlittableJsonTextWriter _writer;
		public event Action<Exception, IncomingReplicationHandler> Failed;

		public IncomingReplicationHandler(IncomingConnectionInfo connectionInfo, 
			TcpConnectionHeaderMessage incomingMessageHeader, 
			JsonOperationContext.MultiDocumentParser multiDocumentParser, 
			DocumentDatabase database, 
			NetworkStream stream)
	    {
		    _connectionInfo = connectionInfo;
		    _incomingMessageHeader = incomingMessageHeader;
		    _multiDocumentParser = multiDocumentParser;
		    _database = database;
		    _stream = stream;
		    _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
			
			_log = _database.LoggerSetup.GetLogger<IncomingReplicationHandler>(_database.Name);
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
	    }

		//TODO : do not forget to add logging and code to record stats
	    private void ReceiveReplicatedDocuments()
	    {
			using (_context)
			using (var writer = new BlittableJsonTextWriter(_context, _stream))
			using (_multiDocumentParser)
			{
				try
				{
					while (!_cts.IsCancellationRequested)
					{
						using (var message = _multiDocumentParser.ParseToMemory("IncomingReplication/read-message"))
						{
							_cts.Token.ThrowIfCancellationRequested();
							string messageType;
							if (!message.TryGet(Constants.MessageType, out messageType))
								throw new InvalidOperationException(
									$"Received replication message without type. All messages must have property {Constants.MessageType}");

							switch (messageType)
							{
								case "GetLastEtag":
									_context.Write(_writer, new DynamicJsonValue
									{
										[Constants.Replication.PropertyNames.LastSentEtag] = GetLastReceivedEtag(Guid.Parse(_incomingMessageHeader.SourceDatabaseId), _context),
										[Constants.MessageType] = "GetLastEtag"
									});
									_writer.Flush();
									break;
								case "ReplicationBatch":

									BlittableJsonReaderArray replicatedDocs;
									if (!message.TryGet(Constants.Replication.PropertyNames.ReplicationBatch, out replicatedDocs))
										throw new InvalidDataException(
											$"Expected the message to have a field with replicated document array, named {Constants.Replication.PropertyNames.ReplicationBatch}. The property wasn't found");

									using (_context.OpenWriteTransaction())
									{
										ReceiveDocuments(_context, replicatedDocs);
										_context.Transaction.Commit();
									}

									break;
								default:
									throw new InvalidOperationException("Unrecognized replication message type.");
							}
						}
					}
				}
				catch (Exception e)
				{
					OnFailed(e, this);
				}
				finally
				{
					_context = null;
					_stream = null;
				}
			}
	    }

	    public string FromToString => $"from {_connectionInfo.SourceDatabaseName} at {_connectionInfo.SourceUrl} (into database {_database.Name})";

	    private long GetLastReceivedEtag(Guid srcDbId, DocumentsOperationContext context)
	    {
		    var dbChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(context);
		    var vectorEntry = dbChangeVector.FirstOrDefault(x => x.DbId == srcDbId);
		    return vectorEntry.Etag;
	    }

		private void ReceiveDocuments(DocumentsOperationContext context, BlittableJsonReaderArray docs)
		{
			var dbChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(context);
			var changeVectorUpdated = false;
			var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
			foreach (BlittableJsonReaderObject doc in docs)
			{
				var changeVector = doc.EnumerateChangeVector();
				foreach (var currentEntry in changeVector)
				{
					Debug.Assert(currentEntry.DbId != Guid.Empty); //should never happen, but..

					//note: documents in a replication batch are ordered in incremental etag order
					maxReceivedChangeVectorByDatabase[currentEntry.DbId] = currentEntry.Etag;
				}

				const string DetachObjectDebugTag = "IncomingDocumentReplication -> Detach object from parent array";
				var detachedDoc = context.ReadObject(doc, DetachObjectDebugTag);
				WriteReceivedDocument(context, detachedDoc);
			}

			//if any of [dbId -> etag] is larger than server pair, update it
			for (int i = 0; i < dbChangeVector.Length; i++)
			{
				long dbEtag;
				if (maxReceivedChangeVectorByDatabase.TryGetValue(dbChangeVector[i].DbId, out dbEtag) == false)
					continue;
				maxReceivedChangeVectorByDatabase.Remove(dbChangeVector[i].DbId);
				if (dbEtag > dbChangeVector[i].Etag)
				{
					changeVectorUpdated = true;
					dbChangeVector[i].Etag = dbEtag;
				}
			}

			if (maxReceivedChangeVectorByDatabase.Count > 0)
			{
				changeVectorUpdated = true;
				var oldSize = dbChangeVector.Length;
				Array.Resize(ref dbChangeVector, oldSize + maxReceivedChangeVectorByDatabase.Count);

				foreach (var kvp in maxReceivedChangeVectorByDatabase)
				{
					dbChangeVector[oldSize++] = new ChangeVectorEntry
					{
						DbId = kvp.Key,
						Etag = kvp.Value,
					};
				}
			}

			if (changeVectorUpdated)
				_database.DocumentsStorage.SetChangeVector(context, dbChangeVector);
		}

		private void WriteReceivedDocument(DocumentsOperationContext context, BlittableJsonReaderObject doc)
		{

			var id = doc.GetIdFromMetadata();
			if (id == null)
				throw new InvalidDataException($"Missing {Constants.DocumentIdFieldName} field from a document; this is not something that should happen...");

			// we need to split this document to an independent blittable document
			// and this time, we'll prepare it for disk.
			doc.PrepareForStorage();
			_database.DocumentsStorage.Put(context, id, null, doc);
		}

		public void Dispose()
	    {		
			_cts.Cancel();
			_incomingThread?.Join();

			//precaution, those should be null by this point
			_context?.Dispose();
			_stream?.Dispose();
	    }

	    protected void OnFailed(Exception exception, IncomingReplicationHandler instance) => Failed?.Invoke(exception, instance);
    }
}
