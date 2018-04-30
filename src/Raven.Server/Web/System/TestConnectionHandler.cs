using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Web.System
{
    public class TestConnectionHandler : RequestHandler
    {
        [RavenAction("/admin/test-connection", "POST", AuthorizationStatus.Operator)]
        public async Task TestConnection()
        {
            var url = GetQueryStringValueAndAssertIfSingleAndNotEmpty("url");
            url = UrlHelper.TryGetLeftPart(url);
            DynamicJsonValue result = null;

            Task<TcpConnectionInfo> connectionInfo = null;

            try
            {
                try
                {
                    var timeout = TimeoutManager.WaitFor(ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan);

                    using (var cts = new CancellationTokenSource(Server.Configuration.Cluster.OperationTimeout.AsTimeSpan))
                    {
                        connectionInfo = ReplicationUtils.GetTcpInfoAsync(url, null, "Test-Connection", Server.Certificate.Certificate,
                            cts.Token);
                    }
                    Task timeoutTask = await Task.WhenAny(timeout, connectionInfo);
                    if (timeoutTask == timeout)
                    {
                        throw new TimeoutException($"Waited for {ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan} to receive tcp info from {url} and got no response");
                    }
                    await connectionInfo;
                }
                catch (Exception e)
                {
                    result = new DynamicJsonValue
                    {
                        [nameof(NodeConnectionTestResult.Success)] = false,
                        [nameof(NodeConnectionTestResult.HTTPSuccess)] = false,
                        [nameof(NodeConnectionTestResult.Error)] = $"An exception was thrown while trying to connect to {url} : {e}"
                    };
                    return;
                }

                try
                {
                    result = await ConnectToClientNodeAsync(connectionInfo.Result, ServerStore.Engine.TcpConnectionTimeout, LoggingSource.Instance.GetLogger("testing-connection", "testing-connection"));
                }
                catch (Exception e)
                {
                    result = new DynamicJsonValue
                    {
                        [nameof(NodeConnectionTestResult.Success)] = false,
                        [nameof(NodeConnectionTestResult.HTTPSuccess)] = true,
                        [nameof(NodeConnectionTestResult.TcpServerUrl)] = connectionInfo.Result.Url,
                        [nameof(NodeConnectionTestResult.Error)] = $"Was able to connect to url {url}, but exception was thrown while trying to connect to TCP port {connectionInfo.Result.Url}: {e}"
                    };
                    return;
                }
            }
            finally
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }            
        }


        private async Task<(TcpClient TcpClient, Stream Connection)> ConnectAndGetNetworkStreamAsync(TcpConnectionInfo tcpConnectionInfo, TimeSpan timeout, Logger log)
        {
            var tcpClient = await TcpUtils.ConnectSocketAsync(tcpConnectionInfo, timeout, log);
            var connection = await TcpUtils.WrapStreamWithSslAsync(tcpClient, tcpConnectionInfo, Server.Certificate.Certificate, timeout);
            return (tcpClient, connection);
        }

        private async Task<DynamicJsonValue> ConnectToClientNodeAsync(TcpConnectionInfo tcpConnectionInfo, TimeSpan timeout, Logger log)
        {
            var (tcpClient, connection) = await ConnectAndGetNetworkStreamAsync(tcpConnectionInfo, timeout, log);
            using (tcpClient)
            {
                var result = new DynamicJsonValue();

                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                using (var writer = new BlittableJsonTextWriter(ctx, connection))
                {
                    WriteOperationHeaderToRemote(writer);
                    using (var responseJson = await ctx.ReadForMemoryAsync(connection, $"TestConnectionHandler/{tcpConnectionInfo.Url}/Read-Handshake-Response"))
                    {
                        var headerResponse = JsonDeserializationServer.TcpConnectionHeaderResponse(responseJson);
                        switch (headerResponse.Status)
                        {
                            case TcpConnectionStatus.Ok:
                                result["Success"] = true;
                                break;
                            case TcpConnectionStatus.AuthorizationFailed:
                                result["Success"] = false;
                                result["Error"] = $"Connection to {tcpConnectionInfo.Url} failed because of authorization failure: {headerResponse.Message}";
                                break;
                            case TcpConnectionStatus.TcpVersionMismatch:
                                WriteOperationHeaderToRemote(writer, drop:true);
                                result["Success"] = false;
                                result["Error"] = $"Connection to {tcpConnectionInfo.Url} failed because of mismatching tcp version {headerResponse.Message}";
                                break;
                        }
                    }

                }
                return result;
            }
        }

        private void WriteOperationHeaderToRemote(BlittableJsonTextWriter writer, bool drop = false)
        {
            var operation = drop ? TcpConnectionHeaderMessage.OperationTypes.Drop : TcpConnectionHeaderMessage.OperationTypes.Heartbeats;
            writer.WriteStartObject();
            {
                writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.Operation));
                writer.WriteString(operation.ToString());
                writer.WriteComma();
                writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.OperationVersion));
                writer.WriteInteger(TcpConnectionHeaderMessage.HeartbeatsTcpVersion);
                writer.WriteComma();
                writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.DatabaseName));
                writer.WriteNull();
            }
            writer.WriteEndObject();
            writer.Flush();
        }
    }

    public class NodeConnectionTestResult
    {
        public bool Success;
        public bool HTTPSuccess;
        public string TcpServerUrl;
        public string Error;
    }
}
