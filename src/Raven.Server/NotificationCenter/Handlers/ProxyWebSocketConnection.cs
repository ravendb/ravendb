using System;
using System.IO;
using System.Net.Http;
using System.Net.WebSockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Extensions;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.NotificationCenter.Handlers
{
    public sealed class ProxyWebSocketConnection : IDisposable
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer(typeof(ProxyWebSocketConnection));

        private readonly CancellationTokenSource _cts;
        private readonly Uri _remoteWebSocketUri;
        private readonly ClientWebSocket _remoteWebSocket;
        private readonly WebSocket _localWebSocket;
        private readonly string _nodeUrl;
        private readonly IMemoryContextPool _contextPool;
        private Task _localToRemote;
        private Task _remoteToLocal;
        private HttpClient _httpClient;

        public ProxyWebSocketConnection(WebSocket localWebSocket, string nodeUrl, string websocketEndpoint, IMemoryContextPool contextPool, CancellationToken token)
        {
            if (string.IsNullOrEmpty(nodeUrl))
                throw new ArgumentException("Node url cannot be null or empty", nameof(nodeUrl));

            if (string.IsNullOrEmpty(websocketEndpoint))
                throw new ArgumentException("Endpoint cannot be null or empty", nameof(websocketEndpoint));

            if (websocketEndpoint.StartsWith("/") == false)
                throw new ArgumentException("Endpoint must starts with '/' character", nameof(websocketEndpoint));

            _localWebSocket = localWebSocket;
            _nodeUrl = nodeUrl;
            _contextPool = contextPool;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            _remoteWebSocketUri = new Uri($"{nodeUrl.Replace("http", "ws", StringComparison.OrdinalIgnoreCase)}{websocketEndpoint}");
            _remoteWebSocket = new ClientWebSocket();
        }

        public Task Establish(X509Certificate2 certificate)
        {
            var handler = DefaultRavenHttpClientFactory.CreateHttpMessageHandler(certificate, setSslProtocols: true, DocumentConventions.DefaultForServer.UseHttpDecompression);

            if (certificate != null)
            {
                var tcpConnection = ReplicationUtils.GetServerTcpInfo(_nodeUrl, $"{nameof(ProxyWebSocketConnection)} to {_nodeUrl}", certificate, _cts.Token);

                var expectedCert = CertificateLoaderUtil.CreateCertificateFromAny(Convert.FromBase64String(tcpConnection.Certificate));

                handler.ServerCertificateCustomValidationCallback = (_, actualCert, _, _) => expectedCert.Equals(actualCert);
            }

            _httpClient = new HttpClient(handler, disposeHandler: true).WithConventions(DocumentConventions.DefaultForServer);

            return _remoteWebSocket.ConnectAsync(_remoteWebSocketUri, _httpClient, _cts.Token);
        }

        public async Task RelayData()
        {
            _localToRemote = ForwardLocalToRemote();
            _remoteToLocal = ForwardRemoteToLocal();

            await Task.WhenAny(_localToRemote, _remoteToLocal);
        }

        private async Task ForwardLocalToRemote()
        {
            using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
            using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment))
            {
                try
                {
                    while (_localWebSocket.State == WebSocketState.Open || _localWebSocket.State == WebSocketState.CloseSent)
                    {
                        if (_remoteToLocal?.IsCompleted == true)
                            break;

                        var buffer = segment.Memory.Memory;

                        var receiveResult = await _localWebSocket.ReceiveAsync(buffer, _cts.Token);

                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            await _localWebSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "NORMAL_CLOSE", _cts.Token);
                            break;
                        }

                        await _remoteWebSocket.SendAsync(buffer.Slice(0, receiveResult.Count), receiveResult.MessageType, receiveResult.EndOfMessage, _cts.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    // ignored
                }
                catch (IOException ex)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Websocket proxy got disconnected (local WS proxy to {_remoteWebSocketUri})", ex);
                }
                catch (Exception ex)
                {
                    // if we received close from the client, we want to ignore it and close the websocket (dispose does it)
                    if (ex is WebSocketException webSocketException
                        && (webSocketException.WebSocketErrorCode == WebSocketError.InvalidState)
                        && (_localWebSocket.State == WebSocketState.Closed || _remoteWebSocket.State == WebSocketState.Closed ||
                            _localWebSocket.State == WebSocketState.CloseReceived || _remoteWebSocket.State == WebSocketState.CloseReceived))
                    {
                        // ignore
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        private async Task ForwardRemoteToLocal()
        {
            using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
            using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment))
            {
                try
                {
                    while (_remoteWebSocket.State == WebSocketState.Open || _remoteWebSocket.State == WebSocketState.CloseSent)
                    {
                        if (_localToRemote?.IsCompleted == true)
                            break;

                        var buffer = segment.Memory.Memory;

                        var receiveResult = await _remoteWebSocket.ReceiveAsync(buffer, _cts.Token).ConfigureAwait(false);

                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            await _remoteWebSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "NORMAL_CLOSE", _cts.Token);
                            break;
                        }

                        await _localWebSocket.SendAsync(buffer.Slice(0, receiveResult.Count), receiveResult.MessageType, receiveResult.EndOfMessage, _cts.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    // ignored
                }
                catch (IOException ex)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Websocket proxy got disconnected ({_remoteWebSocketUri} to local)", ex);
                }
                catch (AggregateException ae)
                {
                    if (IsSocketClosed(ae.ExtractSingleInnerException()))
                    {
                        //ignore
                    }
                    else
                    {
                        throw;
                    }

                }
                catch (Exception ex)
                {
                    if (IsSocketClosed(ex))
                    {
                        // ignore
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            bool IsSocketClosed(Exception ex)
            {
                // if we received close from the client, we want to ignore it and close the websocket (dispose does it)
                return ex is WebSocketException webSocketException
                       && (webSocketException.WebSocketErrorCode == WebSocketError.InvalidState)
                       && (_localWebSocket.State == WebSocketState.Closed || _remoteWebSocket.State == WebSocketState.Closed ||
                           _localWebSocket.State == WebSocketState.CloseReceived || _remoteWebSocket.State == WebSocketState.CloseReceived);
            }
        }

        public void Dispose()
        {
            _cts.Dispose();
            _remoteWebSocket.Dispose();
            _httpClient?.Dispose();
        }
    }
}
