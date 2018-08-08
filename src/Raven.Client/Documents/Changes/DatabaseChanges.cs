using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Security;
using System.Net.WebSockets;
using System.Security.Cryptography.X509Certificates;
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

        private readonly AtomicDictionary<DatabaseConnectionState> _counters = new AtomicDictionary<DatabaseConnectionState>(StringComparer.OrdinalIgnoreCase);
        private int _immediateConnection;

        public DatabaseChanges(RequestExecutor requestExecutor, string databaseName, Action onDispose)
        {
            _requestExecutor = requestExecutor;
            _conventions = requestExecutor.Conventions;
            _database = databaseName;

            _tcs = new TaskCompletionSource<IDatabaseChanges>(TaskCreationOptions.RunContinuationsAsynchronously);
            _cts = new CancellationTokenSource();
            _client = CreateClientWebSocket(_requestExecutor);

            _onDispose = onDispose;
            ConnectionStatusChanged += OnConnectionStatusChanged;

            _task = DoWork();
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
            var counter = GetOrAddConnectionState("prefixes/" + docIdPrefix, "watch-prefix", "unwatch-prefix", docIdPrefix);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => notification.Id != null && notification.Id.StartsWith(docIdPrefix, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<DocumentChange> ForDocumentsInCollection(string collectionName)
        {
            if (collectionName == null)
                throw new ArgumentNullException(nameof(collectionName));

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

        [Obsolete("This method is not supported anymore. Will be removed in next major version of the product.")]
        public IChangesObservable<DocumentChange> ForDocumentsOfType(string typeName)
        {
            if (typeName == null)
                throw new ArgumentNullException(nameof(typeName));

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                DatabaseConnectionState.Dummy,
                notification => false);

            return taskedObservable;
        }

        [Obsolete("This method is not supported anymore. Will be removed in next major version of the product.")]
        public IChangesObservable<DocumentChange> ForDocumentsOfType(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var typeName = _conventions.FindClrTypeName(type);
            return ForDocumentsOfType(typeName);
        }

        [Obsolete("This method is not supported anymore. Will be removed in next major version of the product.")]
        public IChangesObservable<DocumentChange> ForDocumentsOfType<TEntity>()
        {
            var typeName = _conventions.FindClrTypeName(typeof(TEntity));
            return ForDocumentsOfType(typeName);
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

            _task.Wait();

            ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);
            ConnectionStatusChanged -= OnConnectionStatusChanged;

            _onDispose?.Invoke();
        }

        private DatabaseConnectionState GetOrAddConnectionState(string name, string watchCommand, string unwatchCommand, string value)
        {
            bool newValue = false;
            var counter = _counters.GetOrAdd(name, s =>
            {
                async Task OnDisconnect()
                {
                    try
                    {
                        if (Connected)
                            await Send(unwatchCommand, value).ConfigureAwait(false);
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
                    await Send(watchCommand, value).ConfigureAwait(false);
                }

                newValue = true;
                return new DatabaseConnectionState(OnConnect, OnDisconnect);
            });

            // try to reconnect
            if (newValue && Volatile.Read(ref _immediateConnection) != 0)
                counter.Set(counter.OnConnect());

            return counter;
        }

        private async Task Send(string command, string value)
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

        private async Task DoWork()
        {
            try
            {
                await _requestExecutor.GetPreferredNode().ConfigureAwait(false);
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

            var url = new Uri($"{_requestExecutor.Url}/databases/{_database}/changes"
                .ToLower()
                .ToWebSocketPath(), UriKind.Absolute);

            while (_cts.IsCancellationRequested == false)
            {
                try
                {
                    if (Connected == false)
                    {
                        await _client.ConnectAsync(url, _cts.Token).ConfigureAwait(false);

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
                        ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);

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

            ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);
            return true;
        }

        private async Task ProcessChanges()
        {
            using (_requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                while (_cts.IsCancellationRequested == false)
                {
                    var state = new JsonParserState();

                    using (var stream = new WebSocketStream(_client, _cts.Token))
                    using (context.GetManagedBuffer(out var buffer))
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
                            if (await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer).ConfigureAwait(false) == false)
                                continue;

                            if (state.CurrentTokenType == JsonParserToken.EndArray)
                                break;

                            builder.Renew("changes/receive", BlittableJsonDocumentBuilder.UsageMode.None);

                            await UnmanagedJsonParserHelper.ReadObjectAsync(builder, peepingTomStream, parser, buffer).ConfigureAwait(false);

                            var json = builder.CreateReader();

                            try
                            {
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
                                        NotifySubscribers(type, value, _counters.ValuesSnapshot);
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
                case nameof(IndexChange):
                    var indexChange = IndexChange.FromJson(value);
                    foreach (var state in states)
                    {
                        state.Send(indexChange);
                    }
                    break;
                case nameof(OperationStatusChange):
                    var operationStatusChange = OperationStatusChange.FromJson(value, _conventions);
                    foreach (var state in states)
                    {
                        state.Send(operationStatusChange);
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

            foreach (var state in _counters.ValuesSnapshot)
            {
                state.Error(e);
            }
        }
    }
}
