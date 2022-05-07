using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Changes;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Client.Documents.Changes;

internal abstract class AbstractDatabaseChanges<TDatabaseConnectionState>
    where TDatabaseConnectionState : AbstractDatabaseConnectionState
{
    private int _commandId;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private readonly MemoryStream _ms = new();

    protected readonly RequestExecutor RequestExecutor;
    private readonly string _database;

    private readonly Action _onDispose;
    private ClientWebSocket _client;

    private readonly Task _task;
    private readonly CancellationTokenSource _cts;
    private TaskCompletionSource<AbstractDatabaseChanges<TDatabaseConnectionState>> _tcs;

    private readonly ConcurrentDictionary<int, TaskCompletionSource<object>> _confirmations = new();

    protected readonly ConcurrentDictionary<DatabaseChangesOptions, TDatabaseConnectionState> States = new();
    private int _immediateConnection;

    private ServerNode _serverNode;
    private int _nodeIndex;
    private Uri _url;

    protected AbstractDatabaseChanges(RequestExecutor requestExecutor, string databaseName, Action onDispose, string nodeTag)
    {
        RequestExecutor = requestExecutor;
        _database = databaseName;

        _tcs = new TaskCompletionSource<AbstractDatabaseChanges<TDatabaseConnectionState>>(TaskCreationOptions.RunContinuationsAsynchronously);
        _cts = new CancellationTokenSource();
        _client = CreateClientWebSocket(RequestExecutor);

        _onDispose = onDispose;
        ConnectionStatusChanged += OnConnectionStatusChanged;

        _task = DoWork(nodeTag);
    }

    protected abstract TDatabaseConnectionState CreateDatabaseConnectionState(Func<Task> onConnect, Func<Task> onDisconnect);

    private static ClientWebSocket CreateClientWebSocket(RequestExecutor requestExecutor)
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

        _client?.Dispose();

        foreach (var state in States.ForceEnumerateInThreadSafeManner())
        {
            state.Value.Dispose();
        }
        States.Clear();

        try
        {
            _task.Wait();
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

        _onDispose?.Invoke();
    }

    protected TDatabaseConnectionState GetOrAddConnectionState(string name, string watchCommand, string unwatchCommand, string value, string[] values = null)
    {
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

                    foreach (var counter in States)
                    {
                        counter.Value.Set(counter.Value.OnConnect());
                    }

                    ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);
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
                //We don't report this error since we can automatically recover from it and we can't
                // recover from the OnError accessing the faulty WebSocket.
                try
                {
                    if (wasConnected)
                        ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);

                    wasConnected = false;
                    try
                    {
                        _serverNode = await RequestExecutor.HandleServerNotResponsive(_url.AbsoluteUri, _serverNode, _nodeIndex, e).ConfigureAwait(false);
                    }
                    catch (DatabaseDoesNotExistException databaseDoesNotExistException)
                    {
                        e = databaseDoesNotExistException;
                        throw;
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
            _client = CreateClientWebSocket(RequestExecutor);
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
                                if (json.TryGet(nameof(TopologyChange), out bool supports) && supports)
                                {
                                    GetOrAddConnectionState("Topology", "watch-topology-change", "", "");
                                    await RequestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(_serverNode) { TimeoutInMs = 0, ForceUpdate = true, DebugTag = "watch-topology-change" }).ConfigureAwait(false);
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

    private void NotifyAboutError(Exception e)
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
