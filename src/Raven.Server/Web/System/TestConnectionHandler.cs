using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Tcp;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Web.System
{
    public class TestConnectionHandler : RequestHandler
    {
        [RavenAction("/admin/test-connection", "GET", "/admin/test-connection?url={serverUrl:string}&api={apiKey:string}")]
        public async Task TestConnection()
        {
            var url = GetQueryStringValueAndAssertIfSingleAndNotEmpty("url");
            var api = GetStringQueryString("api", false);
            DynamicJsonValue result;

            try
            {
                var timeout = TimeoutManager.WaitFor(ServerStore.Configuration.Cluster.ClusterOperationTimeout.AsTimeSpan);
                var connectionInfo = ReplicationUtils.GetTcpInfoAsync(url, null, api, "Test-Connection");
                if (await Task.WhenAny(timeout, connectionInfo) == timeout)
                {
                    throw new TimeoutException($"Waited for {ServerStore.Configuration.Cluster.ClusterOperationTimeout.AsTimeSpan} to receive tcp info from {url} and got no response");
                }
                using (var tcpClient = new TcpClient())
                {
                    result = await ConnectToClientNodeAsync(connectionInfo.Result, tcpClient);
                }
            }
            catch (Exception e)
            {
                result = new DynamicJsonValue
                {
                    [nameof(NodeConnectionTestResult.Success)] = false,
                    [nameof(NodeConnectionTestResult.Error)] = $"An exception was thrown while trying to connect to {url} : {e.Message}",
                };
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result);
            }
        }


        private static async Task<Stream> ConnectAndGetNetworkStreamAsync(TcpConnectionInfo tcpConnectionInfo, TcpClient tcpClient)
        {
            await TcpUtils.ConnectSocketAsync(tcpConnectionInfo, tcpClient, null);
            return await TcpUtils.WrapStreamWithSslAsync(tcpClient, tcpConnectionInfo);
        }

        private async Task<DynamicJsonValue> ConnectToClientNodeAsync(TcpConnectionInfo tcpConnectionInfo, TcpClient tcpClient)
        {
            var connection = await ConnectAndGetNetworkStreamAsync(tcpConnectionInfo, tcpClient);
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
                        case TcpConnectionHeaderResponse.AuthorizationStatus.Success:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                            result[nameof(NodeConnectionTestResult.Success)] = true;
                            break;
                        default:
                            result[nameof(NodeConnectionTestResult.Success)] = false;
                            result[nameof(NodeConnectionTestResult.Error)] = $"Connection to {tcpConnectionInfo.Url} failed because of bad credentials. AuthorizationStatus: {headerResponse.Status}";
                            break;
                    }
                }

            }

            return result;
        }

        private void WriteOperationHeaderToRemote(BlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();
            {
                writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.Operation));
                writer.WriteString(TcpConnectionHeaderMessage.OperationTypes.Heartbeats.ToString());
                writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.DatabaseName));
                writer.WriteNull();
                writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.AuthorizationToken));
                writer.WriteNull();
            }
            writer.WriteEndObject();
            writer.Flush();
        }
    }

    public class NodeConnectionTestResult
    {
        public bool Success;
        public string Error;
    }
}
