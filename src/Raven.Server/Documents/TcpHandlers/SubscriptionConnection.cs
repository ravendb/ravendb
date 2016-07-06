using System;
using System.IO;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog.Targets.Wrappers;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Server.Documents.Handlers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.TcpHandlers
{
    public class SubscriptionConnection
    {
        private MemoryStream _buffer = new MemoryStream();
        private Stream _networkStream;
        private DocumentDatabase _database;
        private JsonOperationContext _context;
        private readonly TcpClient _tcpClient;
        private BlittableJsonTextWriter _bufferedWriter;

        public SubscriptionConnection(Stream networkStream, DocumentDatabase database, 
            JsonOperationContext context, TcpClient tcpClient)
        {
            _networkStream = networkStream;
            _database = database;
            _context = context;
            _tcpClient = tcpClient;
            _bufferedWriter = new BlittableJsonTextWriter(context, _buffer);
            _unbufferedWriter = new BlittableJsonTextWriter(context, networkStream);
            _logger = database.LoggerSetup.GetLogger<SubscriptionConnection>(database.Name);
        }

        public async Task<bool> InitAsync()
        {
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Starting subscription connection for {_options.SubscriptionId}");
            }
            var subscriptionCommandOptions = await _context.ReadForMemoryAsync(_networkStream, "subscription command");
            _options = JsonDeserialization.SubscriptionConnectionOptions(subscriptionCommandOptions);
            _options.CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
            try
            {
                _database.SubscriptionStorage.AssertSubscriptionConfigExists(_options.SubscriptionId);
            }
            catch (SubscriptionDoesNotExistException e)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Subscription does not exists", e);
                }
                _context.Write(_unbufferedWriter, new DynamicJsonValue
                {
                    ["Type"] = "Error",
                    ["Exception"] = e.ToString()
                });
                return false;
            }
            var state = _database.SubscriptionStorage.OpenSubscription(_options);
            var timeout = 0;

            while (true)
            {
                try
                {
                    _options.DisposeOnDisconnect = await state.RegisterSubscriptionConnection(_options,
                        timeout);

                    await WriteJsonAsync(new DynamicJsonValue
                    {
                        ["Type"] = "CoonectionStatus",
                        ["Status"] = "Accepted"
                    });

                    return true;
                }
                catch (TimeoutException)
                {
                    timeout = 500;
                    await _networkStream.WriteAsync(_heartbeat, 0, _heartbeat.Length);
                }
                catch (SubscriptionInUseException)
                {
                    await WriteJsonAsync(new DynamicJsonValue
                    {
                        ["Type"] = "CoonectionStatus",
                        ["Status"] = "InUse"
                    });
                    return false;
                }
            }
        }

        private async Task WriteJsonAsync(DynamicJsonValue value)
        {
            _context.Write(_bufferedWriter, value);
            await FlushBufferToNetwork();
        }

        private async Task FlushBufferToNetwork()
        {
            ArraySegment<byte> bytes;
            _buffer.TryGetBuffer(out bytes);
            await _networkStream.WriteAsync(bytes.Array, bytes.Offset, bytes.Count);
            _buffer.SetLength(0);
        }

        public static async Task SendSubscriptionDocuments(DocumentDatabase database,
            JsonOperationContext context, NetworkStream stream, TcpClient tcpClient)
        {
            var connection = new SubscriptionConnection(stream, database, context, tcpClient);
            try
            {
                if (await connection.InitAsync() == false)
                    return;

                await connection.ProcessSubscriptionAysnc();
            }
            catch (Exception e)
            {
                if (connection._logger.IsInfoEnabled)
                {
                    connection._logger.Info($"Failed to process subscription {connection._options.SubscriptionId}", e);
                }
                try
                {
                    using (var writer = new BlittableJsonTextWriter(context, stream))
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
                try
                {
                    stream.Dispose();
                }
                catch (Exception)
                {
                }
                try
                {
                    tcpClient.Dispose();
                }
                catch (Exception)
                {
                }
                try
                {
                    context.Dispose();
                }
                catch (Exception)
                {
                }
            }
        }

        private IDisposable RegisterForNotificationOnNewDocuments(SubscriptionCriteria criteria)
        {
            _waitForMoreDocuments = new AsyncManualResetEvent(_options.CancellationTokenSource.Token);
            Action<DocumentChangeNotification> registerNotification = notification =>
            {
                if (notification.CollectionName == criteria.Collection)
                    _waitForMoreDocuments.SetByAsyncCompletion();

            };
            _database.Notifications.OnDocumentChange += registerNotification;
            return new DisposableAction(() =>
            {
                _database.Notifications.OnDocumentChange -= registerNotification;
            });
        }


        private async Task ProcessSubscriptionAysnc()
        {
            DocumentsOperationContext dbContext;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out dbContext))
            {
                long startEtag;
                SubscriptionCriteria criteria;
                _database.SubscriptionStorage.GetCriteriaAndEtag(_options.SubscriptionId, dbContext,
                    out criteria, out startEtag);

                var replyFromClientTask = _context.ReadForMemoryAsync(_networkStream, "client reply");
                try
                {
                    using (RegisterForNotificationOnNewDocuments(criteria))
                    {
                        var spd = SetupFilterScript(criteria);

                        while (_options.CancellationTokenSource.IsCancellationRequested == false)
                        {
                            bool hasDocuments = false;
                            int skipNumber = 0;
                            using (dbContext.OpenReadTransaction())
                            {
                                var documents = _database.DocumentsStorage.GetDocumentsAfter(dbContext,
                                    criteria.Collection,
                                    startEtag + 1, 0, _options.MaxDocsPerBatch);
                                _buffer.SetLength(0);
                                foreach (var doc in documents)
                                {
                                    hasDocuments = true;
                                    startEtag = doc.Etag;
                                    if (DocumentMatchCriteriaScript(spd, dbContext, doc) == false)
                                    {
                                        if (skipNumber++ % _options.MaxDocsPerBatch == 0)
                                        {
                                            await _networkStream.WriteAsync(_heartbeat, 0, _heartbeat.Length);
                                        }
                                        continue;
                                    }
                                    doc.EnsureMetadata();

                                    _context.Write(_bufferedWriter, new DynamicJsonValue
                                    {
                                        ["Type"] = "Data",
                                        ["Data"] = doc.Data
                                    });
                                    if (_buffer.Length > (_options.MaxBatchSize ?? 1024 * 32))
                                    {
                                        await FlushBufferToNetwork();
                                    }
                                    doc.Data.Dispose();
                                }
                                _bufferedWriter.Flush();
                                await FlushBufferToNetwork();

                                _database.SubscriptionStorage.UpdateSubscriptionTimes(_options.SubscriptionId,
                                    updateLastBatch: true, updateClientActivity: false);

                                if (hasDocuments == false)
                                {
                                    if (await WaitForChangedDocuments(replyFromClientTask))
                                        continue;
                                }
                                BlittableJsonReaderObject clientReply;

                                while (true)
                                {
                                    var result = await Task.WhenAny(replyFromClientTask, Task.Delay(TimeSpan.FromSeconds(5)));
                                    if (result == replyFromClientTask)
                                    {
                                        clientReply = await replyFromClientTask;
                                        replyFromClientTask = _context.ReadForMemoryAsync(_networkStream, "client reply");
                                        break;
                                    }
                                    await _networkStream.WriteAsync(_heartbeat, 0, _heartbeat.Length);
                                }

                                //TODO, strongly type with JsonDeserialization

                                string type;
                                clientReply.TryGet("Type", out type);
                                switch (type)
                                {
                                    case "Acknowledge":
                                        long clientEtag;
                                        clientReply.TryGet("Etag", out clientEtag);//todo: error handling
                                        _database.SubscriptionStorage.AcknowledgeBatchProcessed(_options.SubscriptionId,
                                            clientEtag);
                                        await WriteJsonAsync(new DynamicJsonValue
                                        {
                                            ["Type"] = "Confirm",
                                            ["LastAcknowledgedEtag"] = clientEtag
                                        });
                                        break;
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    if (_tcpClient.Connected == false)
                    {
                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info($"Client for {_options.SubscriptionId} disconnected", e);
                        }
                    }
                    throw;
                }
            }
        }

        private async Task<bool> WaitForChangedDocuments(Task pendingReply)
        {
            do
            {
                var hasMoreDocsTask = _waitForMoreDocuments.WaitAsync();
                var resultingTask = await Task.WhenAny(hasMoreDocsTask, pendingReply, Task.Delay(3000));
                if (resultingTask == pendingReply)
                    return false;

                if (hasMoreDocsTask == resultingTask)
                {
                    _waitForMoreDocuments.Reset();
                    return true;
                }

                await _networkStream.WriteAsync(_heartbeat, 0, _heartbeat.Length);
            } while (_options.CancellationTokenSource.IsCancellationRequested == false);
            return false;
        }

        private bool DocumentMatchCriteriaScript(SubscriptionPatchDocument spd, DocumentsOperationContext dbContext,
            Document doc)
        {
            if (spd == null)
                return true;
            try
            {
                return spd.MatchCriteria(dbContext, doc);
            }
            catch (Exception ex)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(
                        $"Criteria script threw exception for subscription {_options.SubscriptionId} for document id {doc.Key}",
                        ex);
                }
                return false;
            }
        }

        private SubscriptionPatchDocument SetupFilterScript(SubscriptionCriteria criteria)
        {
            SubscriptionPatchDocument spd = null;

            if (string.IsNullOrWhiteSpace(criteria.FilterJavaScript) == false)
            {
                spd = new SubscriptionPatchDocument(_database, criteria.FilterJavaScript);
            }
            return spd;
        }

        private static readonly byte[] _heartbeat = Encoding.UTF8.GetBytes("\r\n");
        private Logger _logger;
        private BlittableJsonTextWriter _unbufferedWriter;
        private SubscriptionConnectionOptions _options;
        private AsyncManualResetEvent _waitForMoreDocuments;
    }
}