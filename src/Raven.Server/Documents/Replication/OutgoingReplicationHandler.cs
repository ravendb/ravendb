using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using Raven.Server.Json;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Json.Linq;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using System.Linq;

namespace Raven.Server.Documents.Replication
{
	public class OutgoingReplicationHandler : IDisposable
    {
		private static readonly DocumentConvention _convention = new DocumentConvention();
		private readonly DocumentsOperationContext _context;
		private readonly DocumentDatabase _database;
        private readonly ReplicationDestination _destination;
        private readonly Logger _log;
        private readonly ManualResetEventSlim _waitForChanges = new ManualResetEventSlim(false);
        private readonly CancellationTokenSource _cts;
		private readonly TimeSpan _minimalHeartbeatInterval = TimeSpan.FromSeconds(15);
		private readonly BlittableJsonReaderObject _heartbeatMessage;
		private Thread _sendingThread;

	    private long _lastSentEtag;

        public event EventHandler<Exception> Failed;

		public OutgoingReplicationHandler(
            DocumentDatabase database,
            ReplicationDestination destination)
        {
            _database = database;
	        _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            _destination = destination;
            _log = _database.LoggerSetup.GetLogger<OutgoingReplicationHandler>(_database.Name);
            _database.Notifications.OnDocumentChange += HandleDocumentChange;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
			_heartbeatMessage = _context.ReadObject(new DynamicJsonValue
			{
				[Constants.MessageType] = "Heartbeat"
			}, "heartbeat msg");
		}

        public void Start()
        {
            _sendingThread = new Thread(ReplicateDocuments)
            {
                Name = "Replication from " + _database.Name + " to remote " + _destination.Database + " at " + _destination.Url,
                IsBackground = true
            };
            _sendingThread.Start();
        }


	    private void ReplicateDocuments()
        {
	        try
	        {
		        var connectionInfo = GetTcpInfo();
		        using (var tcpClient = new TcpClient())
		        {
			        ConnectSocket(connectionInfo, tcpClient);
			        using (var stream = tcpClient.GetStream())
			        using (var writer = new BlittableJsonTextWriter(_context, stream))
			        {
				        _context.Write(writer, new DynamicJsonValue
				        {
					        ["DatabaseName"] = _database.Name,
					        ["Operation"] = TcpConnectionHeaderMessage.OperationTypes.Replication.ToString(),
					        ["DatabaseId"] = _database.DbId.ToString(),
				        });

				        _context.Write(writer, new DynamicJsonValue
				        {
					        [Constants.MessageType] = Constants.Replication.MessageTypes.GetLastEtag
				        });
				        writer.Flush();

				        using (var lastEtagMessage = _context.ReadForMemory(stream, "Last etag from server"))
				        {
					        var replicationEtagReply = JsonDeserialization.ReplicationEtagReply(lastEtagMessage);
					        _lastSentEtag = replicationEtagReply.LastSentEtag;
				        }

				        while (_cts.IsCancellationRequested == false)
				        {
					        if (!ExecuteReplicationOnce(writer, stream))
						        using (_context.OpenReadTransaction())
							        if (DocumentsStorage.ReadLastEtag(_context.Transaction.InnerTransaction) < _lastSentEtag)
								        continue;

					        //if this returns false, this means either timeout or canceled token is activated                    
							while (_waitForChanges.Wait(_minimalHeartbeatInterval, _cts.Token) == false)
						        SendHeartbeat(stream);
				        }
			        }
		        }
	        }
	        catch (OperationCanceledException)
	        {
				if (_log.IsInfoEnabled)
					_log.Info($"Operation canceled on replication thread ({FromToString}). Stopped the thread.");
	        }
            catch (Exception e)
            { 
				if(_log.IsInfoEnabled)
					_log.Info($"Unexpected exception occured on replication thread ({FromToString}). Stopped the thread.", e);
                Failed?.Invoke(this, e);
            }
        }

	    private string FromToString => $"from {_database.ResourceName} to {_destination.Database} at {_destination.Url}";

		private void SendHeartbeat(NetworkStream stream)
	    {
		    try
		    {
			    using (var writer = new BlittableJsonTextWriter(_context, stream))
				    writer.WriteObjectOrdered(_heartbeatMessage);
		    }
			catch (Exception e)
			{
				if (_log.IsInfoEnabled)
					_log.Info($"Sending heartbeat failed. ({_database.Name})", e);
				throw;
			}
		}

		private bool ExecuteReplicationOnce(BlittableJsonTextWriter writer, NetworkStream stream)
	    {
		    //just for shorter code
		    var documentStorage = _database.DocumentsStorage;
		    using (_context.OpenReadTransaction())
		    {
			    //TODO: make replication batch size configurable
			    //also, perhaps there should be timers/heuristics
			    //that would dynamically resize batch size
			    var replicationBatch =
				    documentStorage
					    .GetDocumentsAfter(_context, _lastSentEtag, 0, 1024)
					    .Where(x => !x.Key.ToString().StartsWith("Raven/"))
					    .ToList();

			    //the filtering here will need to be reworked -> it is not efficient
			    //TODO: do not forget to make version of GetDocumentsAfter with a prefix filter	
			    //alternatively 1 -> create efficient StartsWith() for LazyString				
			    //alternatively 2 -> create a "filter system" that would abstract the logic -> what documents 
			    //should and should not be replicated
			    if (replicationBatch.Count == 0)
				    return false;

			    _cts.Token.ThrowIfCancellationRequested();

			    SendDocuments(writer, stream, replicationBatch);
			    return true;
		    }
	    }

	    private void SendDocuments(BlittableJsonTextWriter writer, NetworkStream stream, IEnumerable<Document> docs)
	    {
		    if (docs == null) //precaution, should never happen
				throw new ArgumentNullException(nameof(docs));

		    if (_log.IsInfoEnabled)
			    _log.Info($"Starting sending replication batch ({_database.Name})");

		    var sw = Stopwatch.StartNew();
		    writer.WriteStartObject();

		    writer.WritePropertyName(_context.GetLazyStringForFieldWithCaching(Constants.MessageType));
		    writer.WriteString(_context.GetLazyStringForFieldWithCaching(
			    Constants.Replication.MessageTypes.ReplicationBatch));

		    writer.WritePropertyName(
			    _context.GetLazyStringForFieldWithCaching(
				    Constants.Replication.PropertyNames.ReplicationBatch));
		    _lastSentEtag = writer.WriteDocuments(_context, docs, false);
		    writer.WriteEndObject();
		    writer.Flush();
			sw.Stop();

		    // number of docs, first / last etag, size, time
		    if (_log.IsInfoEnabled)
			    _log.Info(
				    $"Finished sending replication batch. Sent {docs.Count()} documents in {sw.ElapsedMilliseconds} ms. First sent etag = {docs.First().Etag}, last sent etag = {_lastSentEtag}");

		    using (var replicationBatchReplyMessage = _context.ReadForMemory(stream, "replication acknowledge message"))
		    {
			    var replicationBatchReply = JsonDeserialization.ReplicationBatchReply(replicationBatchReplyMessage);

			    if (_log.IsInfoEnabled)
			    {
				    switch (replicationBatchReply.Type)
				    {
					    case ReplicationBatchReply.ReplyType.Success:
								_log.Info($"Received reply for replication batch from {_destination.Database} at {_destination.Url}. Everything is ok.");
							break;
					    case ReplicationBatchReply.ReplyType.Failure:
						    _log.Info(
							    $"Received reply for replication batch from {_destination.Database} at {_destination.Url}. There has been a failure, error string received : {replicationBatchReply.Error}");
							throw new InvalidOperationException($"Received failure reply for replication batch. Error string received = {replicationBatchReply.Error}");
					    default:
						    throw new ArgumentOutOfRangeException("replicationBatchReply.Type", "Received reply for replication batch with unrecognized type...");
				    }
			    }
		    }
		}	   

	    private void ConnectSocket(TcpConnectionInfo connection, TcpClient tcpClient)
		{
			var host = new Uri(connection.Url).Host;
			try
			{
				tcpClient.ConnectAsync(host, connection.Port).Wait();
			}
			catch (SocketException e)
			{
				if (_log.IsInfoEnabled)
					_log.Info($"Failed to connect to remote replication destination {host}:{connection.Port}. Socket Error Code = {e.SocketErrorCode}", e);
				throw;
			}
			catch (Exception e)
			{
				if (_log.IsInfoEnabled)
					_log.Info($"Failed to connect to remote replication destination {host}:{connection.Port}", e);
				throw;
			}
		}

	    private TcpConnectionInfo GetTcpInfo()
		{
			using (var request = _database.HttpRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, string.Format("{0}/info/tcp", 
				MultiDatabase.GetRootDatabaseUrl(_destination.Url)), 
				HttpMethod.Get,
				// TODO: OperationCredentials should be just ApiKey
				new OperationCredentials(_destination.ApiKey, CredentialCache.DefaultCredentials), 
				_convention)))
			{
				var result = request.ReadResponseJson();
				return _convention.CreateSerializer().Deserialize<TcpConnectionInfo>(new RavenJTokenReader(result));
			}
		}

		private void HandleDocumentChange(DocumentChangeNotification notification)
        {
            _waitForChanges.Set();
        }

        public void Dispose()
        {
            _database.Notifications.OnDocumentChange -= HandleDocumentChange;
            _cts.Cancel();
            _sendingThread?.Join();
			_context.Dispose();
        }	   
	}
}