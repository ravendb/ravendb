using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
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
using Raven.Server.Json;
using Raven.Server.Rachis.Remote;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class NodeDebugHandler : ServerRequestHandler
    {
        [RavenAction("/admin/debug/node/clear-http-clients-pool", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task ClearHttpClientsPool()
        {
            DefaultRavenHttpClientFactory.Instance.Clear();

            return NoContent(HttpStatusCode.OK);
        }

        [RavenAction("/admin/debug/node/remote-connections", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task ListRemoteConnections()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var write = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
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
            await using (var write = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                context.Write(write, ServerStore.Engine.InMemoryDebug.ToJson());
            }
        }

        [RavenAction("/admin/debug/node/state-change-history", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task GetStateChangeHistory()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
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
            var setStatusCodeOnError = GetBoolValueQueryString("setStatusCodeOnError", required: false) ?? false;
            var topology = ServerStore.GetClusterTopology();
            var tasks = new List<Task<PingResult>>();
            var tasksResults = new List<PingResult>();

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

            while (tasks.Count > 0)
            {
                var task = await Task.WhenAny(tasks);
                tasks.Remove(task);
                tasksResults.Add(task.Result);

                if (setStatusCodeOnError && task.Result.HasErrors)
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Result");

                writer.WriteStartArray();

                var isFirst = true;

                foreach (var taskResult in tasksResults)
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    context.Write(writer, taskResult.ToJson());
                    await writer.MaybeFlushAsync();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }
        }

        private class SetupAliveCommand : RavenCommand
        {
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/setup/alive";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };

                return request;
            }
        }

        internal class PingResult : IDynamicJsonValueConvertible
        {
            public string Url;
            public SetupAliveInfo SetupAlive;
            public TcpInfo TcpInfo;
            public List<string> Log;

            public bool HasErrors => SetupAlive.Error != null || TcpInfo.Error != null;

            public PingResult()
            {
                SetupAlive = new SetupAliveInfo();
                TcpInfo = new TcpInfo();
            }

            public DynamicJsonValue ToJson()
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(Url)] = Url,
                    [nameof(SetupAlive)] = SetupAlive.ToJson(),
                    [nameof(TcpInfo)] = TcpInfo.ToJson()
                };

                if (Log != null)
                {
                    djv[nameof(Log)] = new DynamicJsonArray(Log);
                }

                return djv;
            }
        }

        internal class SetupAliveInfo : IDynamicJson
        {
            public long Time;
            public string Error;

            public DynamicJsonValue ToJson()
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(Time)] = Time,
                    [nameof(Error)] = Error
                };

                return djv;
            }
        }

        internal class TcpInfo : IDynamicJson
        {
            public long TcpInfoTime;
            public long SendTime;
            public long ReceiveTime;
            public string Error;

            public DynamicJsonValue ToJson()
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(TcpInfoTime)] = TcpInfoTime,
                    [nameof(SendTime)] = SendTime,
                    [nameof(ReceiveTime)] = ReceiveTime,
                    [nameof(Error)] = Error,
                };

                return djv;
            }
        }

        private async Task<PingResult> PingOnce(string url)
        {
            var sp = Stopwatch.StartNew();
            var log = new List<string>();
            var result = new PingResult { Url = url };

            try
            {
                using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(url, Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new SetupAliveCommand();
                    await requestExecutor.ExecuteAsync(command, context, token: cts.Token);
                    result.SetupAlive.Time = sp.ElapsedMilliseconds;
                }
            }
            catch (Exception e)
            {
                result.SetupAlive.Error = e.ToString();
            }

            try
            {
                using (var cts = new CancellationTokenSource(ServerStore.Engine.TcpConnectionTimeout))
                {
                    var info = await ReplicationUtils.GetServerTcpInfoAsync(url, "PingTest", ServerStore.Engine.ClusterCertificate, cts.Token);
                    result.TcpInfo.TcpInfoTime = sp.ElapsedMilliseconds;

                    using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        using (await TcpUtils.ConnectSecuredTcpSocket(info, ServerStore.Engine.ClusterCertificate, Server.CipherSuitesPolicy,
                                   TcpConnectionHeaderMessage.OperationTypes.Ping, NegotiationCallback, context, ServerStore.Engine.TcpConnectionTimeout, log, cts.Token))
                        {
                        }

                        async Task<TcpConnectionHeaderMessage.SupportedFeatures> NegotiationCallback(string curUrl, TcpConnectionInfo tcpInfo, Stream stream,
                            JsonOperationContext ctx, List<string> logs = null)
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
                                result.TcpInfo.SendTime = sp.ElapsedMilliseconds;
                                logs?.Add($"message sent to url {curUrl} at {result.TcpInfo.SendTime} ms.");
                                ctx.Write(writer, msgJson);
                            }

                            using (var rawResponse = await ctx.ReadForMemoryAsync(stream, "cluster-ConnectToPeer-header-response"))
                            {
                                TcpConnectionHeaderResponse response = JsonDeserializationServer.TcpConnectionHeaderResponse(rawResponse);
                                result.TcpInfo.ReceiveTime = sp.ElapsedMilliseconds;
                                string message;
                                logs?.Add($"response received from url {curUrl} at {result.TcpInfo.ReceiveTime} ms.");

                                switch (response.Status)
                                {
                                    case TcpConnectionStatus.Ok:
                                        logs?.Add($"Successfully negotiated with {url}.");
                                        break;

                                    case TcpConnectionStatus.AuthorizationFailed:
                                        message = $"Connection to {url} failed because of authorization failure: {response.Message}";
                                        result.TcpInfo.Error = message;
                                        logs?.Add(message);
                                        throw new AuthorizationException(message);

                                    case TcpConnectionStatus.TcpVersionMismatch:
                                        message = $"Connection to {url} failed because of mismatching tcp version: {response.Message}";
                                        result.TcpInfo.Error = message;
                                        logs?.Add(message);
                                        throw new AuthorizationException(message);

                                    case TcpConnectionStatus.InvalidNetworkTopology:
                                        message = $"Connection to {url} failed because of {nameof(TcpConnectionStatus.InvalidNetworkTopology)} error: {response.Message}";
                                        result.TcpInfo.Error = message;
                                        logs?.Add(message);
                                        throw new InvalidNetworkTopologyException(message);
                                }
                            }

                            return null;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                result.TcpInfo.Error ??= e.ToString();
                log.Add($"Error occurred while attempting to negotiate with the server. {e.Message}");
            }

            result.Log = log;

            return result;
        }
    }
}
