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
		private readonly DocumentDatabase _database;
        private readonly ReplicationDestination _destination;
        private readonly Logger _log;
        private readonly ManualResetEventSlim _waitForChanges = new ManualResetEventSlim(false);
        private readonly CancellationTokenSource _cts;
		private readonly TimeSpan _minimalHeartbeatInterval = TimeSpan.FromSeconds(15);
		private BlittableJsonReaderObject _heartbeatMessage;
		private Thread _sendingThread;

	    private long _lastSentEtag;

        public event EventHandler<Exception> Failed;

		public OutgoingReplicationHandler(
            DocumentDatabase database,
            ReplicationDestination destination)
        {
            _database = database;	        			
			_destination = destination;
            _log = _database.LoggerSetup.GetLogger<OutgoingReplicationHandler>(_database.Name);
            _database.Notifications.OnDocumentChange += HandleDocumentChange;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
			
		}

        public void Start()
        {
            _sendingThread = new Thread(ReplicateDocuments)
            {
                Name = $"Outgoing replication {FromToString}",
                IsBackground = true
            };
            _sendingThread.Start();
        }

		//TODO : add code to record stats and maybe additional logging
		private void ReplicateDocuments()
        {
	        try
	        {
		        var connectionInfo = _database.GetTcpInfo(_destination.Url, _destination.ApiKey);
		        using (var tcpClient = new TcpClient())
		        {
					DocumentsOperationContext context;
					ConnectSocket(connectionInfo, tcpClient);
			        using (var stream = tcpClient.GetStream())
			        using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
			        using (var writer = new BlittableJsonTextWriter(context, stream))
			        using (var parser = context.ParseMultiFrom(stream))
			        {
						//cache heartbeat msg
						_heartbeatMessage = context.ReadObject(new DynamicJsonValue
						{
							[Constants.MessageType] = "Heartbeat"
						}, $"heartbeat msg {FromToString}");

						//send initial connection information
						context.Write(writer, new DynamicJsonValue
				        {
							["DatabaseName"] = _destination.Database,
					        ["Operation"] = TcpConnectionHeaderMessage.OperationTypes.Replication.ToString(),
					        ["SourceDatabaseId"] = _database.DbId.ToString(),
					        ["SourceDatabaseName"] = _database.Name,
					        ["SourceUrl"] = _database.Url
				        });

						//start request/response for fetching last etag
				        context.Write(writer, new DynamicJsonValue
				        {
					        [Constants.MessageType] = "GetLastEtag"
						});
				        writer.Flush();

						using (var lastEtagMessage = parser.ParseToMemory($"Last etag from server {FromToString}"))
				        {
					        var replicationEtagReply = JsonDeserialization.ReplicationEtagReply(lastEtagMessage);
					        _lastSentEtag = replicationEtagReply.LastSentEtag;
				        }

				        while (_cts.IsCancellationRequested == false)
				        {
					        if (!ExecuteReplicationOnce(context, writer, parser))
						        using (context.OpenReadTransaction())
							        if (DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction) < _lastSentEtag)
								        continue;

					        //if this returns false, this means either timeout or canceled token is activated                    
					        while (_waitForChanges.Wait(_minimalHeartbeatInterval, _cts.Token) == false)
						        SendHeartbeat(writer);
				        }
			        }
		        }
	        }
	        catch (OperationCanceledException)
	        {
				_log.InfoIfEnabled($"Operation canceled on replication thread ({FromToString}). Stopped the thread.");
	        }
            catch (Exception e)
            { 
				_log.InfoIfEnabled($"Unexpected exception occured on replication thread ({FromToString}). Stopped the thread.", e);
                Failed?.Invoke(this, e);
            }
        }

	    private string FromToString => $"from {_database.ResourceName} to {_destination.Database} at {_destination.Url}";

		private void SendHeartbeat(BlittableJsonTextWriter writer)
	    {
		    try
		    {
			    writer.WriteObjectOrdered(_heartbeatMessage);
				writer.Flush();
		    }
			catch (Exception e)
			{
				_log.InfoIfEnabled($"Sending heartbeat failed. ({FromToString})", e);
				throw;
			}
		}

		
		private bool ExecuteReplicationOnce(
			DocumentsOperationContext context,
			BlittableJsonTextWriter writer, 
			JsonOperationContext.MultiDocumentParser parser)
	    {
		    //just for shorter code
		    var documentStorage = _database.DocumentsStorage;
		    using (context.OpenReadTransaction())
		    {
			    //TODO: make replication batch size configurable
			    //also, perhaps there should be timers/heuristics
			    //that would dynamically resize batch size
			    var replicationBatch =
				    documentStorage
					    .GetDocumentsAfter(context, _lastSentEtag, 0, 1024)
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

			    SendDocuments(context, writer, parser, replicationBatch);
			    return true;
		    }
	    }

		//TODO: add replication batch format in comments here		
	    private void SendDocuments(
			DocumentsOperationContext context,
			BlittableJsonTextWriter writer, 
			JsonOperationContext.MultiDocumentParser parser, 
			IEnumerable<Document> docs)
	    {
		    if (docs == null) //precaution, should never happen
				throw new ArgumentNullException(nameof(docs));

			_log.InfoIfEnabled($"Starting sending replication batch ({_database.Name})");

		    var sw = Stopwatch.StartNew();
		    writer.WriteStartObject();

		    writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(Constants.MessageType));
		    writer.WriteString(context.GetLazyStringForFieldWithCaching(
			    "ReplicationBatch"));

		    writer.WritePropertyName(
			    context.GetLazyStringForFieldWithCaching(
				    Constants.Replication.PropertyNames.ReplicationBatch));
		    var docsArray = docs.ToArray();
		    _lastSentEtag = writer.WriteDocuments(context, docsArray, false);
		    writer.WriteEndObject();
		    writer.Flush();
			sw.Stop();

		    // number of docs, first / last etag, size, time
			_log.InfoIfEnabled(
				    $"Finished sending replication batch. Sent {docsArray.Length} documents in {sw.ElapsedMilliseconds} ms. First sent etag = {docsArray[0].Etag}, last sent etag = {_lastSentEtag}");

		    using (var replicationBatchReplyMessage = parser.ParseToMemory("replication acknowledge message"))
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
						    throw new ArgumentOutOfRangeException("replicationBatchReply.Type", "Received reply for replication batch with unrecognized type... got " + replicationBatchReply.Type);
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
				_log.InfoIfEnabled($"Failed to connect to remote replication destination {host}:{connection.Port}. Socket Error Code = {e.SocketErrorCode}", e);
				throw;
			}
			catch (Exception e)
			{
				
				_log.InfoIfEnabled($"Failed to connect to remote replication destination {host}:{connection.Port}", e);
				throw;
			}
		}	   

		private void HandleDocumentChange(DocumentChangeNotification notification) => _waitForChanges.Set();

	    public void Dispose()
        {
            _database.Notifications.OnDocumentChange -= HandleDocumentChange;
            _cts.Cancel();
            _sendingThread?.Join();		
        }	   
	}
}