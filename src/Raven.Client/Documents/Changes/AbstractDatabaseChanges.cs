using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IO;
using Raven.Client.Exceptions.Changes;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Client.Documents.Changes;

internal abstract class AbstractDatabaseChanges<TDatabaseConnectionState> : IDisposable
    where TDatabaseConnectionState : AbstractDatabaseConnectionState
{
    private int _commandId;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private readonly MemoryStream _ms = RecyclableMemoryStreamFactory.GetMemoryStream();

    protected readonly RequestExecutor RequestExecutor;
    private readonly string _database;

    private readonly Action _onDispose;
    protected readonly string _nodeTag;
    private readonly bool _throttleConnection;

    private ClientWebSocket _client => _lazyClient.Value;
    private Lazy<ClientWebSocket> _lazyClient;

    private Lazy<Task> _task;
    private readonly CancellationTokenSource _cts;
    private TaskCompletionSource<AbstractDatabaseChanges<TDatabaseConnectionState>> _tcs;

    private readonly ConcurrentDictionary<int, TaskCompletionSource<object>> _confirmations = new();

    protected readonly ConcurrentDictionary<DatabaseChangesOptions, TDatabaseConnectionState> States = new();
    private int _immediateConnection;

    private readonly TaskCompletionSource<ChangesSupportedFeatures> _supportedFeaturesTcs = new();
    internal Task<ChangesSupportedFeatures> GetSupportedFeaturesAsync() => _supportedFeaturesTcs.Task;

    private ServerNode _serverNode;
    private int _nodeIndex;
    private Uri _url;

    protected AbstractDatabaseChanges(RequestExecutor requestExecutor, string databaseName, Action onDispose, string nodeTag, bool throttleConnection)
    {
        RequestExecutor = requestExecutor;
        _database = databaseName;

        _tcs = new TaskCompletionSource<AbstractDatabaseChanges<TDatabaseConnectionState>>(TaskCreationOptions.RunContinuationsAsynchronously);
        _cts = new CancellationTokenSource();
        _lazyClient = new Lazy<ClientWebSocket>(() => CreateClientWebSocket(RequestExecutor), LazyThreadSafetyMode.ExecutionAndPublication);

        _onDispose = onDispose;
        _nodeTag = nodeTag;
        _throttleConnection = throttleConnection;
        ConnectionStatusChanged += OnConnectionStatusChanged;

        GetSupportedFeaturesAsync().ContinueWith(async t =>
        {
            if (t.Result.TopologyChange == false)
                return;

            GetOrAddConnectionState("Topology", "watch-topology-change", "", "");
            await RequestExecutor
                .UpdateTopologyAsync(
                    new RequestExecutor.UpdateTopologyParameters(_serverNode) { TimeoutInMs = 0, ForceUpdate = true, DebugTag = "watch-topology-change" })
                .ConfigureAwait(false);
        });

        _task = new Lazy<Task>(() =>
        {
            var t = DoWork(_nodeTag);
            t.ContinueWith(_ => Dispose());
            return t;
        }, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    private void EnsureRunning() => _ = _task.Value;

    protected abstract TDatabaseConnectionState CreateDatabaseConnectionState(Func<Task> onConnect, Func<Task> onDisconnect);

    protected virtual ClientWebSocket CreateClientWebSocket(RequestExecutor requestExecutor)
    {
        var clientWebSocket = new ClientWebSocket();
#if NET7_0_OR_GREATER
        if (requestExecutor.Conventions.HttpVersion != null)
            clientWebSocket.Options.HttpVersion = requestExecutor.Conventions.HttpVersion;

        if (requestExecutor.Conventions.HttpVersionPolicy != null)
            clientWebSocket.Options.HttpVersionPolicy = requestExecutor.Conventions.HttpVersionPolicy.Value;
#else
        if (requestExecutor.Certificate != null)
            clientWebSocket.Options.ClientCertificates.Add(requestExecutor.Certificate);

#if NETCOREAPP3_1_OR_GREATER
        if (RequestExecutor.HasServerCertificateCustomValidationCallback)
        {
            clientWebSocket.Options.RemoteCertificateValidationCallback += RequestExecutor.OnServerCertificateCustomValidationCallback;
        }
#endif
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
                _tcs = new TaskCompletionSource<AbstractDatabaseChanges<TDatabaseConnectionState>>(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public bool Connected => _client?.State == WebSocketState.Open;

    protected Task<AbstractDatabaseChanges<TDatabaseConnectionState>> EnsureConnectedNowAsync()
    {
        EnsureRunning();
        return _tcs.Task;
    }

    public event EventHandler ConnectionStatusChanged;

    public event Action<Exception> OnError;

    public void Dispose()
    {
        foreach (var confirmation in _confirmations)
        {
            confirmation.Value.TrySetCanceled();
        }

        _cts.Cancel();

        if (_lazyClient.IsValueCreated)
            _client?.Dispose();

        foreach (var state in States.ForceEnumerateInThreadSafeManner())
        {
            state.Value.Dispose();
        }
        States.Clear();

        try
        {
            if (_task.IsValueCreated)
                _task.Value.Wait();
        }
        catch
        {
            // we're disposing the document store
            // nothing we can do here
        }

        try
        {
            ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);
        }
        catch
        {
            // we are disposing
        }

        ConnectionStatusChanged -= OnConnectionStatusChanged;

        _ms?.Dispose();

        _onDispose?.Invoke();
    }

    protected TDatabaseConnectionState GetOrAddConnectionState(string name, string watchCommand, string unwatchCommand, string value, string[] values = null)
    {
        EnsureRunning();

        bool newValue = false;
        var counter = States.GetOrAdd(new DatabaseChangesOptions
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
                        await SendAsync(unwatchCommand, value, values).ConfigureAwait(false);
                }
                catch (WebSocketException)
                {
                    // if we are not connected then we unsubscribed already
                    // because connections drops with all subscriptions
                }

                if (States.TryRemove(s, out var state))
                    state.Dispose();
            }

            async Task OnConnect()
            {
                await SendAsync(watchCommand, value, values).ConfigureAwait(false);
            }

            newValue = true;
            return CreateDatabaseConnectionState(OnConnect, OnDisconnect);
        });

        if (_tcs.Task.IsFaulted)
        {
            counter.Set(_tcs.Task);
        }
        else if (newValue && Volatile.Read(ref _immediateConnection) != 0)
        {
            // try to reconnect
            counter.Set(counter.OnConnect());
        }

        return counter;
    }

    private async Task SendAsync(string command, string value, string[] values)
    {
        var taskCompletionSource = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        int currentCommandId;
        await _semaphore.WaitAsync(_cts.Token).ConfigureAwait(false);
        try
        {
            currentCommandId = ++_commandId;
            using (RequestExecutor.ContextPool.AllocateOperationContext(out var context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, _ms))
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

    public class OnReconnect : EventArgs
    {
        public static OnReconnect Instance = new OnReconnect();
    }

    private async Task DoWork(string nodeTag)
    {
        try
        {
            var task = nodeTag == null || RequestExecutor.Conventions.DisableTopologyUpdates
                ? RequestExecutor.GetPreferredNode()
                : RequestExecutor.GetRequestedNode(nodeTag);

            (_nodeIndex, _serverNode) = await task.ConfigureAwait(false);
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

        var timerInSec = 1;
        var wasConnected = false;
        while (_cts.IsCancellationRequested == false)
        {
            try
            {
                if (Connected == false)
                {
                    _url = new Uri($"{_serverNode.Url}/databases/{_database}/changes?throttleConnection={_throttleConnection}"
                        .ToLower()
                        .ToWebSocketPath(), UriKind.Absolute);

                    using (var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token))
                    {
                        timeoutCts.CancelAfter(TimeSpan.FromSeconds(15));
#if NET7_0_OR_GREATER
                        await _client.ConnectAsync(_url, RequestExecutor.HttpClient, timeoutCts.Token).ConfigureAwait(false);
#else
                        await _client.ConnectAsync(_url, timeoutCts.Token).ConfigureAwait(false);
#endif
                    }

                    timerInSec = 1;
                    wasConnected = true;
                    Interlocked.Exchange(ref _immediateConnection, 1);

                    foreach (var counter in States)
                    {
                        counter.Value.Set(counter.Value.OnConnect());
                    }

                    ConnectionStatusChanged?.Invoke(this, OnReconnect.Instance);
                }
                await ProcessChanges().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_cts.Token.IsCancellationRequested)
            {
                // disposing
                return;
            }
            catch (ChangeProcessingException)
            {
                continue;
            }
            catch (Exception e)
            {
                // we don't report this error since we can automatically recover from it,
                // and we can't recover from the OnError accessing the faulty WebSocket.
                try
                {
                    NotifyAboutReconnection(e);

                    if (wasConnected)
                        ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);

                    wasConnected = false;
                    try
                    {
                        // If node tag is provided we should not failover to a different node
                        // Failing over will create a mismatch if the operation is created and monitored on the provided node tag
                        if (string.IsNullOrEmpty(_nodeTag))
                            _serverNode = await RequestExecutor.HandleServerNotResponsive(_url.AbsoluteUri, _serverNode, _nodeIndex, e).ConfigureAwait(false);
                        else
                            await RequestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(_serverNode) { TimeoutInMs = 0, ForceUpdate = true, DebugTag = $"changes-api-connection-failure-{_database}" }).ConfigureAwait(false);
                    }
                    catch (DatabaseDoesNotExistException databaseDoesNotExistException)
                    {
                        e = databaseDoesNotExistException;
                        throw;
                    }
                    catch (Exception)
                    {
                        // we don't want to stop observing for changes if the server is down. we will wait for it to be up.
                    }

                    if (ReconnectClient() == false)
                        return;
                }
                catch
                {
                    // we couldn't reconnect
                    NotifyAboutError(e);
                    _tcs.TrySetException(e);
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
                timerInSec = Math.Min(timerInSec * 2, 60);
                await TimeoutManager.WaitFor(TimeSpan.FromSeconds(timerInSec), _cts.Token).ConfigureAwait(false);
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
            _lazyClient = new Lazy<ClientWebSocket>(() => CreateClientWebSocket(RequestExecutor));
        }

        return true;
    }

    private async Task ProcessChanges()
    {
        using (RequestExecutor.ContextPool.AllocateOperationContext(out var context))
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
                                if (json.TryGet(nameof(TopologyChange), out bool _))
                                {
                                    var supportedFeatures = JsonDeserializationClient.ChangesSupportedFeatures(json);
                                    _supportedFeaturesTcs.TrySetResult(supportedFeatures);
                                    continue;
                                }

                                if (json.TryGet("Type", out string type) == false)
                                    continue;

                                switch (type)
                                {
                                    case "Error":
                                        ProcessErrorNotification(json);
                                        break;

                                    case "Confirm":
                                        ProcessConfirmationNotification(json);
                                        break;

                                    default:
                                        ProcessNotification(type, json);
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

    protected abstract void ProcessNotification(string type, BlittableJsonReaderObject change);

    private void ProcessConfirmationNotification(BlittableJsonReaderObject json)
    {
        if (json.TryGet("CommandId", out int commandId) &&
            _confirmations.TryRemove(commandId, out var tcs))
        {
            tcs.TrySetResult(null);
        }
    }

    private void ProcessErrorNotification(BlittableJsonReaderObject json)
    {
        json.TryGet("Exception", out string exceptionAsString);
        NotifyAboutError(new Exception(exceptionAsString));
    }

    internal virtual void NotifyAboutReconnection(Exception e)
    {
    }

    internal void NotifyAboutError(Exception e)
    {
        if (_cts.Token.IsCancellationRequested)
            return;

        OnError?.Invoke(e);

        foreach (var state in States.ForceEnumerateInThreadSafeManner())
        {
            state.Value.Error(e);
        }
    }
}
