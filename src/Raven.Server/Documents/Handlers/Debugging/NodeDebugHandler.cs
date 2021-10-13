using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Json;
using Raven.Server.Rachis.Remote;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class NodeDebugHandler : RequestHandler
    {
        [RavenAction("/admin/debug/node/remote-connections", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task ListRemoteConnections()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var write = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(write,
                    new DynamicJsonValue
                    {
                        ["Remote-Connections"] = new DynamicJsonArray(RemoteConnection.RemoteConnectionsList
                            .Select(connection => new DynamicJsonValue
                            {
                                [nameof(RemoteConnection.RemoteConnectionInfo.Caller)] = connection.Caller,
                                [nameof(RemoteConnection.RemoteConnectionInfo.Term)] = connection.Term,
                                [nameof(RemoteConnection.RemoteConnectionInfo.Destination)] = connection.Destination,
                                [nameof(RemoteConnection.RemoteConnectionInfo.StartAt)] = connection.StartAt,
                                ["Duration"] = DateTime.UtcNow - connection.StartAt,
                                [nameof(RemoteConnection.RemoteConnectionInfo.Number)] = connection.Number,
                            }))
                    });
            }
        }

        [RavenAction("/admin/debug/node/engine-logs", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task ListRecentEngineLogs()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var write = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(write, ServerStore.Engine.InMemoryDebug.ToJson());
            }
        }

        [RavenAction("/admin/debug/node/state-change-history", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task GetStateChangeHistory()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray("States", ServerStore.Engine.PrevStates.Select(s => s.ToString()));
                writer.WriteEndObject();
            }
        }

        [RavenAction("/admin/debug/node/ping", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task PingTest()
        {
            var dest = GetStringQueryString("url", false) ?? GetStringQueryString("node", false);
            var topology = ServerStore.GetClusterTopology();
            var tasks = new List<Task<PingResult>>();
            if (string.IsNullOrEmpty(dest))
            {
                foreach (var node in topology.AllNodes)
                {
                    tasks.Add(PingOnce(node.Value));
                }
            }
            else
            {
                var url = topology.GetUrlFromTag(dest);
                tasks.Add(PingOnce(url ?? dest));
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Result");

                writer.WriteStartArray();
                while (tasks.Count > 0)
                {
                    var task = await Task.WhenAny(tasks);
                    tasks.Remove(task);
                    context.Write(writer, task.Result.ToJson());
                    if (tasks.Count > 0)
                    {
                        writer.WriteComma();
                    }
                    await writer.MaybeFlushAsync();
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
        }

        internal class PingResult : IDynamicJsonValueConvertible
        {
            public string Url;
            public long TcpInfoTime;
            public long SendTime;
            public long ReceiveTime;
            public string Error;
            public List<string> Log = null;

            public DynamicJsonValue ToJson()
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(Url)] = Url,
                    [nameof(TcpInfoTime)] = TcpInfoTime,
                    [nameof(SendTime)] = SendTime,
                    [nameof(ReceiveTime)] = ReceiveTime,
                    [nameof(Error)] = Error
                };

                if(Log != null)
                {
                    djv[nameof(Log)] = new DynamicJsonArray(Log);
                }

                return djv;
            }
        }

        private async Task<PingResult> PingOnce(string url)
        {
            var sp = Stopwatch.StartNew();
            var result = new PingResult
            {
                Url = url
            };

            using (var cts = new CancellationTokenSource(ServerStore.Engine.TcpConnectionTimeout))
            {
                var info = await ReplicationUtils.GetTcpInfoAsync(url, null, "PingTest", ServerStore.Engine.ClusterCertificate, cts.Token);
                result.TcpInfoTime = sp.ElapsedMilliseconds;
                
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var log = new List<string>();

                    using (await TcpUtils.ConnectSecuredTcpSocket(info, ServerStore.Engine.ClusterCertificate, Server.CipherSuitesPolicy,
                        TcpConnectionHeaderMessage.OperationTypes.Ping, NegotiationCallback, context, ServerStore.Engine.TcpConnectionTimeout, log, cts.Token))
                    {
                    }

                    result.Log = log;

                    async Task<TcpConnectionHeaderMessage.SupportedFeatures> NegotiationCallback(string curUrl, TcpConnectionInfo tcpInfo, Stream stream,
                        JsonOperationContext ctx, List<string> logs = null)
                    {
                        try
                        {
                            var msg = new DynamicJsonValue
                            {
                                [nameof(TcpConnectionHeaderMessage.DatabaseName)] = null,
                                [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Ping,
                                [nameof(TcpConnectionHeaderMessage.OperationVersion)] = -1,
                                [nameof(TcpConnectionHeaderMessage.ServerId)] = tcpInfo.ServerId
                            };

                            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                            using (var msgJson = ctx.ReadObject(msg, "message"))
                            {
                                result.SendTime = sp.ElapsedMilliseconds;
                                logs?.Add($"message sent to url {curUrl} at {result.SendTime} ms.");
                                ctx.Write(writer, msgJson);
                            }

                            using (var rawResponse = await ctx.ReadForMemoryAsync(stream, "cluster-ConnectToPeer-header-response"))
                            {
                                TcpConnectionHeaderResponse response = JsonDeserializationServer.TcpConnectionHeaderResponse(rawResponse);
                                result.ReceiveTime = sp.ElapsedMilliseconds;
                                logs?.Add($"response received from url {curUrl} at {result.ReceiveTime} ms.");

                                switch (response.Status)
                                {
                                    case TcpConnectionStatus.Ok:
                                        result.Error = null;
                                        logs?.Add($"Successfully negotiated with {url}.");
                                        break;

                                    case TcpConnectionStatus.AuthorizationFailed:
                                        result.Error = $"Connection to {url} failed because of authorization failure: {response.Message}";
                                        logs?.Add(result.Error);
                                        throw new AuthorizationException(result.Error);

                                    case TcpConnectionStatus.TcpVersionMismatch:
                                        result.Error = $"Connection to {url} failed because of mismatching tcp version: {response.Message}";
                                        logs?.Add(result.Error);
                                        throw new AuthorizationException(result.Error);

                                    case TcpConnectionStatus.InvalidNetworkTopology:
                                        result.Error = $"Connection to {url} failed because of {nameof(TcpConnectionStatus.InvalidNetworkTopology)} error: {response.Message}";
                                        logs?.Add(result.Error);
                                        throw new InvalidNetworkTopologyException(result.Error);

                                }
                            }
                        }
                        catch (Exception e)
                        {
                            result.Error = e.ToString();
                            logs?.Add($"Error occurred while attempting to negotiate with the server. {e.Message}");
                            throw;
                        }

                        return null;
                    }
                }
            }
            return result;
        }
    }
}
