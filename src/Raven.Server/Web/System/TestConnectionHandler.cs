using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Web.System
{
    public sealed class TestConnectionHandler : ServerRequestHandler
    {
        [RavenAction("/admin/test-connection", "POST", AuthorizationStatus.Operator)]
        public async Task TestConnection()
        {
            var url = GetQueryStringValueAndAssertIfSingleAndNotEmpty("url");
            var database = GetStringQueryString("database", required: false); // can be null
            var bidirectional = GetBoolValueQueryString("bidirectional", required: false);

            url = UrlHelper.TryGetLeftPart(url);

            // test connection to the remote node
            var result = await ServerStore.TestConnectionToRemote(url, database);

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                // test the connection from the remote node to this one
                if (bidirectional == true && result.Success)
                {
                    using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(url, ServerStore.Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
                    {
                        result = await ServerStore.TestConnectionFromRemote(requestExecutor, context, url);
                    }
                }

                context.Write(writer, result.ToJson());
            }
        }

        public static async Task ConnectToClientNodeAsync(RavenServer server, TcpConnectionInfo tcpConnectionInfo, TimeSpan timeout, RavenLogger log, string database,
            NodeConnectionTestResult result, CancellationToken token = default)
        {
            List<string> negLogs = new();

            using (server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                using var tcpSocketResult = await TcpUtils.ConnectSecuredTcpSocket(tcpConnectionInfo, server.Certificate.Certificate, server.CipherSuitesPolicy,
                    TcpConnectionHeaderMessage.OperationTypes.TestConnection,
                    NegotiateWithRemote, ctx, timeout, negLogs, token);
            }

            result.Log = negLogs;

            async Task<TcpConnectionHeaderMessage.SupportedFeatures> NegotiateWithRemote(string url, TcpConnectionInfo info, Stream stream, JsonOperationContext context, List<string> logs = null)
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
                {
                    await WriteOperationHeaderToRemote(writer, TcpConnectionHeaderMessage.OperationTypes.TestConnection, database, info.ServerId);
                    using (var responseJson = await context.ReadForMemoryAsync(stream, $"TestConnectionHandler/{url}/Read-Handshake-Response"))
                    {
                        var headerResponse = JsonDeserializationServer.TcpConnectionHeaderResponse(responseJson);
                        switch (headerResponse.Status)
                        {
                            case TcpConnectionStatus.Ok:
                                result.Success = true;
                                result.Error = null;
                                logs?.Add($"Successfully negotiated with {url}.");
                                break;

                            case TcpConnectionStatus.AuthorizationFailed:
                                result.Success = false;
                                result.Error = $"Connection to {url} failed because of authorization failure: {headerResponse.Message}";
                                logs?.Add(result.Error);
                                throw new AuthorizationException(result.Error);

                            case TcpConnectionStatus.TcpVersionMismatch:
                                result.Success = false;
                                result.Error = $"Connection to {url} failed because of mismatching tcp version: {headerResponse.Message}";
                                logs?.Add(result.Error);
                                await WriteOperationHeaderToRemote(writer, TcpConnectionHeaderMessage.OperationTypes.Drop, database, info.ServerId);
                                throw new AuthorizationException(result.Error);
                            case TcpConnectionStatus.InvalidNetworkTopology:
                                result.Success = false;
                                result.Error = $"Connection to {url} failed because of {nameof(TcpConnectionStatus.InvalidNetworkTopology)} error: {headerResponse.Message}";
                                logs?.Add(result.Error);
                                throw new InvalidNetworkTopologyException(result.Error);
                        }
                    }
                }
                return null;
            }
        }

        private static async ValueTask WriteOperationHeaderToRemote(AsyncBlittableJsonTextWriter writer, TcpConnectionHeaderMessage.OperationTypes operation,
            string databaseName, string destinationServerId)
        {
            writer.WriteStartObject();
            
            writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.Operation));
            writer.WriteString(operation.ToString());
            writer.WriteComma();
            writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.OperationVersion));
            writer.WriteInteger(TcpConnectionHeaderMessage.GetOperationTcpVersion(operation));
            writer.WriteComma();
            writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.DatabaseName));
            writer.WriteString(databaseName);
            writer.WriteComma();
            writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.ServerId));
            writer.WriteString(destinationServerId);
            
            writer.WriteEndObject();
            await writer.FlushAsync();
        }
    }

    public sealed class NodeConnectionTestResult : IDynamicJson
    {
        public bool Success;
        public bool HTTPSuccess;
        public string TcpServerUrl;
        public string Error;
        public List<string> Log;

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue
            {
                [nameof(Success)] = Success,
                [nameof(HTTPSuccess)] = HTTPSuccess,
                [nameof(TcpServerUrl)] = TcpServerUrl,
                [nameof(Error)] = Error
            };

            if (Log != null)
            {
                djv[nameof(Log)] = new DynamicJsonArray(Log);
            }

            return djv;
        }

        public static string GetError(string source, string dest)
        {
            return $"You are able to reach '{dest}', but the remote node failed to reach you back on '{source}'.{Environment.NewLine}" +
                   $"Please validate the correctness of your '{RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)}', '{RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)}' configuration and the firewall rules.{Environment.NewLine}" +
                   $"Please visit https://ravendb.net/l/QUPWS7/4.1 for the RavenDB setup instructions.";
        }
    }

    public sealed class TestNodeConnectionCommand : RavenCommand<NodeConnectionTestResult>
    {
        private readonly string _url;
        private readonly string _database;
        private readonly bool _bidirectional;

        public TestNodeConnectionCommand(string destination, string database = null, bool bidirectional = false)
        {
            _url = destination;
            _database = database;
            _bidirectional = bidirectional;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/test-connection?url={_url}";
            if (string.IsNullOrEmpty(_database) == false)
            {
                url += $"&database={_database}";
            }
            if (_bidirectional)
            {
                url += "&bidirectional=true";
            }
            return new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationServer.NodeConnectionTestResult(response);
        }

        public override bool IsReadRequest => true;
    }
}
