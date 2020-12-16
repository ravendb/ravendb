using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Changes;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Client.Documents.Changes
{
    public class DatabaseChanges : IDatabaseChanges
    {
        private int _commandId;
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
        private readonly MemoryStream _ms = new MemoryStream();

        private readonly RequestExecutor _requestExecutor;
        private readonly DocumentConventions _conventions;
        private readonly string _database;

        private readonly Action _onDispose;
        private ClientWebSocket _client;

        private readonly Task _task;
        private readonly CancellationTokenSource _cts;
        private TaskCompletionSource<IDatabaseChanges> _tcs;

        private readonly ConcurrentDictionary<int, TaskCompletionSource<object>> _confirmations = new ConcurrentDictionary<int, TaskCompletionSource<object>>();

        private readonly ConcurrentDictionary<DatabaseChangesOptions, DatabaseConnectionState> _counters = new ConcurrentDictionary<DatabaseChangesOptions, DatabaseConnectionState>();
        private int _immediateConnection;

        private ServerNode _serverNode;
        private int _nodeIndex;
        private Uri _url;

        public DatabaseChanges(RequestExecutor requestExecutor, string databaseName, Action onDispose, string nodeTag)
        {
            _requestExecutor = requestExecutor;
            _conventions = requestExecutor.Conventions;
            _database = databaseName;

            _tcs = new TaskCompletionSource<IDatabaseChanges>(TaskCreationOptions.RunContinuationsAsynchronously);
            _cts = new CancellationTokenSource();
            _client = CreateClientWebSocket(_requestExecutor);

            _onDispose = onDispose;
            ConnectionStatusChanged += OnConnectionStatusChanged;

            _task = DoWork(nodeTag);
        }

        public static ClientWebSocket CreateClientWebSocket(RequestExecutor requestExecutor)
        {
            var clientWebSocket = new ClientWebSocket();
            if (requestExecutor.Certificate != null)
                clientWebSocket.Options.ClientCertificates.Add(requestExecutor.Certificate);

#if NETCOREAPP
            if (RequestExecutor.HasServerCertificateCustomValidationCallback)
            {
                clientWebSocket.Options.RemoteCertificateValidationCallback += RequestExecutor.OnServerCertificateCustomValidationCallback;
            }
#endif
            return clientWebSocket;
        }

        private void OnConnectionStatusChanged(object sender, EventArgs e)
        {
            try
            {
                _semaphore.Wait(_cts.Token);
            }
            catch (OperationCanceledException)
            {
                // disposing
                return;
            }

            try
            {
                if (Connected)
                {
                    _tcs.TrySetResult(this);
                    return;
                }

                if (_tcs.Task.Status == TaskStatus.RanToCompletion)
                    _tcs = new TaskCompletionSource<IDatabaseChanges>(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public bool Connected => _client?.State == WebSocketState.Open;

        public Task<IDatabaseChanges> EnsureConnectedNow()
        {
            return _tcs.Task;
        }

        public event EventHandler ConnectionStatusChanged;

        public IChangesObservable<IndexChange> ForIndex(string indexName)
        {
            if (string.IsNullOrWhiteSpace(indexName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(indexName));

            var counter = GetOrAddConnectionState("indexes/" + indexName, "watch-index", "unwatch-index", indexName);

            var taskedObservable = new ChangesObservable<IndexChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(notification.Name, indexName, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public Exception GetLastConnectionStateException()
        {
            foreach (var counter in _counters)
            {
                var valueLastException = counter.Value.LastException;
                if (valueLastException != null)
                    return valueLastException;
            }

            return null;
        }

        public IChangesObservable<DocumentChange> ForDocument(string docId)
        {
            if (string.IsNullOrWhiteSpace(docId))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(docId));

            var counter = GetOrAddConnectionState("docs/" + docId, "watch-doc", "unwatch-doc", docId);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(notification.Id, docId, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<DocumentChange> ForAllDocuments()
        {
            var counter = GetOrAddConnectionState("all-docs", "watch-docs", "unwatch-docs", null);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => true);

            return taskedObservable;
        }

        public IChangesObservable<OperationStatusChange> ForOperationId(long operationId)
        {
            var counter = GetOrAddConnectionState("operations/" + operationId, "watch-operation", "unwatch-operation", operationId.ToString());

            var taskedObservable = new ChangesObservable<OperationStatusChange, DatabaseConnectionState>(
                counter,
                notification => notification.OperationId == operationId);

            return taskedObservable;
        }

        public IChangesObservable<OperationStatusChange> ForAllOperations()
        {
            var counter = GetOrAddConnectionState("all-operations", "watch-operations", "unwatch-operations", null);

            var taskedObservable = new ChangesObservable<OperationStatusChange, DatabaseConnectionState>(
                counter,
                notification => true);

            return taskedObservable;
        }

        public IChangesObservable<IndexChange> ForAllIndexes()
        {
            var counter = GetOrAddConnectionState("all-indexes", "watch-indexes", "unwatch-indexes", null);

            var taskedObservable = new ChangesObservable<IndexChange, DatabaseConnectionState>(
                counter,
                notification => true);

            return taskedObservable;
        }

        public IChangesObservable<DocumentChange> ForDocumentsStartingWith(string docIdPrefix)
        {
            if (string.IsNullOrWhiteSpace(docIdPrefix))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(docIdPrefix));

            var counter = GetOrAddConnectionState("prefixes/" + docIdPrefix, "watch-prefix", "unwatch-prefix", docIdPrefix);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => notification.Id != null && notification.Id.StartsWith(docIdPrefix, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<DocumentChange> ForDocumentsInCollection(string collectionName)
        {
            if (string.IsNullOrWhiteSpace(collectionName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(collectionName));

            var counter = GetOrAddConnectionState("collections/" + collectionName, "watch-collection", "unwatch-collection", collectionName);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(collectionName, notification.CollectionName, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<DocumentChange> ForDocumentsInCollection<TEntity>()
        {
            var collectionName = _conventions.GetCollectionName(typeof(TEntity));
            return ForDocumentsInCollection(collectionName);
        }

        public IChangesObservable<CounterChange> ForAllCounters()
        {
            var counter = GetOrAddConnectionState("all-counters", "watch-counters", "unwatch-counters", null);

            var taskedObservable = new ChangesObservable<CounterChange, DatabaseConnectionState>(
                counter,
                notification => true);

            return taskedObservable;
        }

        public IChangesObservable<CounterChange> ForCounter(string counterName)
        {
            if (string.IsNullOrWhiteSpace(counterName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(counterName));

            var counter = GetOrAddConnectionState($"counter/{counterName}", "watch-counter", "unwatch-counter", counterName);

            var taskedObservable = new ChangesObservable<CounterChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(counterName, notification.Name, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<CounterChange> ForCounterOfDocument(string documentId, string counterName)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));
            if (string.IsNullOrWhiteSpace(counterName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(counterName));

            var counter = GetOrAddConnectionState($"document/{documentId}/counter/{counterName}", "watch-document-counter", "unwatch-document-counter", value: null, values: new[] { documentId, counterName });

            var taskedObservable = new ChangesObservable<CounterChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(counterName, notification.Name, StringComparison.OrdinalIgnoreCase) && string.Equals(documentId, notification.DocumentId, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<CounterChange> ForCountersOfDocument(string documentId)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));

            var counter = GetOrAddConnectionState($"document/{documentId}/counter", "watch-document-counters", "unwatch-document-counters", documentId);

            var taskedObservable = new ChangesObservable<CounterChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(documentId, notification.DocumentId, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<TimeSeriesChange> ForAllTimeSeries()
        {
            var counter = GetOrAddConnectionState("all-timeseries", "watch-all-timeseries", "unwatch-all-timeseries", null);

            var taskedObservable = new ChangesObservable<TimeSeriesChange, DatabaseConnectionState>(
                counter,
                notification => true);

            return taskedObservable;
        }

        public IChangesObservable<TimeSeriesChange> ForTimeSeries(string timeSeriesName)
        {
            if (string.IsNullOrWhiteSpace(timeSeriesName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(timeSeriesName));

            var counter = GetOrAddConnectionState($"timeseries/{timeSeriesName}", "watch-timeseries", "unwatch-timeseries", timeSeriesName);

            var taskedObservable = new ChangesObservable<TimeSeriesChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(timeSeriesName, notification.Name, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<TimeSeriesChange> ForTimeSeriesOfDocument(string documentId, string timeSeriesName)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));
            if (string.IsNullOrWhiteSpace(timeSeriesName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(timeSeriesName));

            var counter = GetOrAddConnectionState($"document/{documentId}/timeseries/{timeSeriesName}", "watch-document-timeseries", "unwatch-document-timeseries", value: null, values: new[] { documentId, timeSeriesName });

            var taskedObservable = new ChangesObservable<TimeSeriesChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(timeSeriesName, notification.Name, StringComparison.OrdinalIgnoreCase) && string.Equals(documentId, notification.DocumentId, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<TimeSeriesChange> ForTimeSeriesOfDocument(string documentId)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));

            var counter = GetOrAddConnectionState($"document/{documentId}/timeseries", "watch-all-document-timeseries", "unwatch-all-document-timeseries", documentId);

            var taskedObservable = new ChangesObservable<TimeSeriesChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(documentId, notification.DocumentId, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public event Action<Exception> OnError;

        public void Dispose()
        {
            foreach (var confirmation in _confirmations)
            {
                confirmation.Value.TrySetCanceled();
            }

            _cts.Cancel();

            _client?.Dispose();

            _counters.Clear();

            try
            {
                _task.Wait();
            }
            catch
            {
                // we're disposing the document store
                // nothing we can do here
            }

            ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);
            ConnectionStatusChanged -= OnConnectionStatusChanged;

            _onDispose?.Invoke();
        }

        private DatabaseConnectionState GetOrAddConnectionState(string name, string watchCommand, string unwatchCommand, string value, string[] values = null)
        {
            bool newValue = false;
            var counter = _counters.GetOrAdd(new DatabaseChangesOptions
            {
                DatabaseName = name,
                NodeTag = null
            }, s =>
            {
                async Task OnDisconnect()
                {
                    try
                    {
                        if (Connected)
                            await Send(unwatchCommand, value, values).ConfigureAwait(false);
                    }
                    catch (WebSocketException)
                    {
                        // if we are not connected then we unsubscribed already
                        // because connections drops with all subscriptions
                    }

                    if (_counters.TryRemove(s, out var state))
                        state.Dispose();
                }

                async Task OnConnect()
                {
                    await Send(watchCommand, value, values).ConfigureAwait(false);
                }

                newValue = true;
                return new DatabaseConnectionState(OnConnect, OnDisconnect);
            });

            // try to reconnect
            if (newValue && Volatile.Read(ref _immediateConnection) != 0)
                counter.Set(counter.OnConnect());

            return counter;
        }

        private async Task Send(string command, string value, string[] values)
        {
            var taskCompletionSource = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            int currentCommandId;
            await _semaphore.WaitAsync(_cts.Token).ConfigureAwait(false);
            try
            {
                currentCommandId = ++_commandId;
                using (_requestExecutor.ContextPool.AllocateOperationContext(out var context))
                using (var writer = new BlittableJsonTextWriter(context, _ms))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("CommandId");
                    writer.WriteInteger(currentCommandId);

                    writer.WriteComma();
                    writer.WritePropertyName("Command");
                    writer.WriteString(command);
                    writer.WriteComma();

                    writer.WritePropertyName("Param");
                    writer.WriteString(value);

                    if (values != null && values.Length > 0)
                    {
                        writer.WriteComma();
                        writer.WriteArray("Params", values);
                    }

                    writer.WriteEndObject();
                }

                _ms.TryGetBuffer(out var buffer);

                _confirmations.TryAdd(currentCommandId, taskCompletionSource);

                await _client.SendAsync(buffer, WebSocketMessageType.Text, endOfMessage: true, cancellationToken: _cts.Token).ConfigureAwait(false);
            }
            finally
            {
                _ms.SetLength(0);
                _semaphore.Release();
            }

            if (await taskCompletionSource.Task.WaitWithTimeout(TimeSpan.FromSeconds(15)).ConfigureAwait(false) == false)
            {
                throw new TimeoutException("Did not get a confirmation for command #" + currentCommandId);
            }
        }

        private async Task DoWork(string nodeTag)
        {
            try
            {
                (_nodeIndex, _serverNode) = nodeTag == null || _requestExecutor.Conventions.DisableTopologyUpdates
                    ? await _requestExecutor.GetPreferredNode().ConfigureAwait(false)
                    : await _requestExecutor.GetRequestedNode(nodeTag).ConfigureAwait(false);
            }
            catch (OperationCanceledException e)
            {
                NotifyAboutError(e);
                _tcs.TrySetCanceled();
                return;
            }
            catch (Exception e)
            {
                ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);
                NotifyAboutError(e);
                _tcs.TrySetException(e);
                return;
            }

            var wasConnected = false;
            while (_cts.IsCancellationRequested == false)
            {
                try
                {
                    if (Connected == false)
                    {
                        _url = new Uri($"{_serverNode.Url}/databases/{_database}/changes"
                            .ToLower()
                            .ToWebSocketPath(), UriKind.Absolute);

                        await _client.ConnectAsync(_url, _cts.Token).ConfigureAwait(false);
                        wasConnected = true;
                        Interlocked.Exchange(ref _immediateConnection, 1);

                        foreach (var counter in _counters)
                        {
                            counter.Value.Set(counter.Value.OnConnect());
                        }

                        ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);
                    }

                    await ProcessChanges().ConfigureAwait(false);
                }
                catch (OperationCanceledException e)
                {
                    NotifyAboutError(e);
                    return;
                }
                catch (ChangeProcessingException)
                {
                    continue;
                }
                catch (Exception e)
                {
                    //We don't report this error since we can automatically recover from it and we can't
                    // recover from the OnError accessing the faulty WebSocket.
                    try
                    {
                        if (wasConnected)
                            ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);

                        wasConnected = false;
                        try
                        {
                            _serverNode = await _requestExecutor.HandleServerNotResponsive(_url.AbsoluteUri, _serverNode, _nodeIndex, e).ConfigureAwait(false);
                        }
                        catch (Exception)
                        {
                            //We don't want to stop observe for changes if server down. we will wait for one to be up
                        }

                        if (ReconnectClient() == false)
                            return;
                    }
                    catch
                    {
                        // we couldn't reconnect
                        NotifyAboutError(e);
                        throw;
                    }
                }
                finally
                {
                    foreach (var confirmation in _confirmations)
                    {
                        confirmation.Value.TrySetCanceled();
                    }
                    _confirmations.Clear();
                }

                try
                {
                    await TimeoutManager.WaitFor(TimeSpan.FromSeconds(1), _cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        private bool ReconnectClient()
        {
            if (_cts.IsCancellationRequested)
                return false;

            using (_client)
            {
                Interlocked.Exchange(ref _immediateConnection, 0);
                _client = CreateClientWebSocket(_requestExecutor);
            }

            return true;
        }

        private async Task ProcessChanges()
        {
            using (_requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                while (_cts.IsCancellationRequested == false)
                {
                    context.Reset();
                    context.Renew();

                    var state = new JsonParserState();

                    using (var stream = new WebSocketStream(_client, _cts.Token))
                    using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer buffer))
                    using (var parser = new UnmanagedJsonParser(context, state, "changes/receive"))
                    using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "readArray/singleResult", parser, state))
                    using (var peepingTomStream = new PeepingTomStream(stream, context))
                    {
                        if (await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer).ConfigureAwait(false) == false)
                            continue;

                        if (state.CurrentTokenType != JsonParserToken.StartArray)
                            continue;

                        while (true)
                        {
                            builder.Reset();
                            builder.Renew("changes/receive", BlittableJsonDocumentBuilder.UsageMode.None);

                            if (await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer).ConfigureAwait(false) == false)
                                continue;

                            if (state.CurrentTokenType == JsonParserToken.EndArray)
                                break;

                            await UnmanagedJsonParserHelper.ReadObjectAsync(builder, peepingTomStream, parser, buffer).ConfigureAwait(false);

                            using (var json = builder.CreateReader())
                            {
                                try
                                {
                                    if (json.TryGet(nameof(TopologyChange), out bool supports) && supports)
                                    {
                                        GetOrAddConnectionState("Topology", "watch-topology-change", "", "");
                                        await _requestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(_serverNode) { TimeoutInMs = 0, ForceUpdate = true, DebugTag = "watch-topology-change" }).ConfigureAwait(false);
                                        continue;
                                    }

                                    if (json.TryGet("Type", out string type) == false)
                                        continue;

                                    switch (type)
                                    {
                                        case "Error":
                                            json.TryGet("Exception", out string exceptionAsString);
                                            NotifyAboutError(new Exception(exceptionAsString));
                                            break;
                                        case "Confirm":
                                            if (json.TryGet("CommandId", out int commandId) &&
                                                _confirmations.TryRemove(commandId, out var tcs))
                                            {
                                                tcs.TrySetResult(null);
                                            }

                                            break;
                                        default:
                                            json.TryGet("Value", out BlittableJsonReaderObject value);
                                            NotifySubscribers(type, value, _counters.ForceEnumerateInThreadSafeManner().Select(x => x.Value).ToList());
                                            break;
                                    }
                                }
                                catch (Exception e)
                                {
                                    NotifyAboutError(e);
                                    throw new ChangeProcessingException(e);
                                }
                            }
                        }
                    }
                }
            }
        }

        private void NotifySubscribers(string type, BlittableJsonReaderObject value, List<DatabaseConnectionState> states)
        {
            switch (type)
            {
                case nameof(DocumentChange):
                    var documentChange = DocumentChange.FromJson(value);
                    foreach (var state in states)
                    {
                        state.Send(documentChange);
                    }
                    break;
                case nameof(CounterChange):
                    var counterChange = CounterChange.FromJson(value);
                    foreach (var state in states)
                    {
                        state.Send(counterChange);
                    }
                    break;
                case nameof(TimeSeriesChange):
                    var timeSeriesChange = TimeSeriesChange.FromJson(value);
                    foreach (var state in states)
                    {
                        state.Send(timeSeriesChange);
                    }
                    break;
                case nameof(IndexChange):
                    var indexChange = IndexChange.FromJson(value);
                    foreach (var state in states)
                    {
                        state.Send(indexChange);
                    }
                    break;
                case nameof(OperationStatusChange):
                    var operationStatusChange = OperationStatusChange.FromJson(value);
                    foreach (var state in states)
                    {
                        state.Send(operationStatusChange);
                    }
                    break;
                case nameof(TopologyChange):
                    var topologyChange = TopologyChange.FromJson(value);

                    var requestExecutor = _requestExecutor;
                    if (requestExecutor != null)
                    {
                        var node = new ServerNode
                        {
                            Url = topologyChange.Url,
                            Database = topologyChange.Database
                        };

                        requestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(node)
                        {
                            TimeoutInMs = 0,
                            ForceUpdate = true,
                            DebugTag = "topology-change-notification"
                        }).ConfigureAwait(false);
                    }
                    break;
                default:
                    throw new NotSupportedException(type);
            }
        }

        private void NotifyAboutError(Exception e)
        {
            if (_cts.Token.IsCancellationRequested)
                return;

            OnError?.Invoke(e);

            foreach (var state in _counters.ForceEnumerateInThreadSafeManner())
            {
                state.Value.Error(e);
            }
        }
    }
}
