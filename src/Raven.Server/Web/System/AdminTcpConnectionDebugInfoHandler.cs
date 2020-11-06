using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class AdminTcpConnectionDebugInfoHandler : RequestHandler
    {
        [RavenAction("/admin/debug/info/tcp/stats", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task Statistics()
        {
            var properties = IPGlobalProperties.GetIPGlobalProperties();
            var ipv4Stats = properties.GetTcpIPv4Statistics();
            var ipv6Stats = properties.GetTcpIPv6Statistics();

            var djv = new DynamicJsonValue
            {
                ["IPv4"] = ToDynamic(ipv4Stats),
                ["IPv6"] = ToDynamic(ipv6Stats)
            };

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, djv);
            }

            return Task.CompletedTask;

            static DynamicJsonValue ToDynamic(TcpStatistics stats)
            {
                var result = new DynamicJsonValue();
                if (stats == null)
                    return result;

                result[nameof(stats.ConnectionsAccepted)] = stats.ConnectionsAccepted;
                result[nameof(stats.ConnectionsInitiated)] = stats.ConnectionsInitiated;
                result[nameof(stats.CumulativeConnections)] = stats.CumulativeConnections;
                result[nameof(stats.CurrentConnections)] = stats.CurrentConnections;
                result[nameof(stats.ErrorsReceived)] = stats.ErrorsReceived;
                result[nameof(stats.FailedConnectionAttempts)] = stats.FailedConnectionAttempts;
                result[nameof(stats.MaximumConnections)] = stats.MaximumConnections;
                result[nameof(stats.MaximumTransmissionTimeout)] = stats.MaximumTransmissionTimeout;
                result[nameof(stats.MinimumTransmissionTimeout)] = stats.MinimumTransmissionTimeout;
                result[nameof(stats.ResetConnections)] = stats.ResetConnections;
                result[nameof(stats.ResetsSent)] = stats.ResetsSent;
                result[nameof(stats.SegmentsReceived)] = stats.SegmentsReceived;
                result[nameof(stats.SegmentsResent)] = stats.SegmentsResent;
                result[nameof(stats.SegmentsSent)] = stats.SegmentsSent;

                return result;
            }
        }

        [RavenAction("/admin/debug/info/tcp/active-connections", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task ActiveConnections()
        {
            var properties = IPGlobalProperties.GetIPGlobalProperties();
            var connections = properties.GetActiveTcpConnections();

            var djv = new DynamicJsonValue
            {
                ["TotalConnections"] = connections?.Length ?? 0,
                ["Connections"] = ToDynamic(connections)
            };

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, djv);
            }

            return Task.CompletedTask;

            static DynamicJsonValue ToDynamic(TcpConnectionInformation[] connections)
            {
                var result = new DynamicJsonValue();
                if (connections == null || connections.Length == 0)
                    return result;

                foreach (var g in connections.GroupBy(x => x.State))
                    result[g.Key.ToString()] = ToDynamicArray(g);

                return result;
            }

            static DynamicJsonArray ToDynamicArray(IEnumerable<TcpConnectionInformation> connections)
            {
                var array = new DynamicJsonArray();
                if (connections == null)
                    return array;

                foreach (var connection in connections)
                {
                    array.Add(new DynamicJsonValue
                    {
                        [nameof(connection.LocalEndPoint)] = connection.LocalEndPoint.ToString(),
                        [nameof(connection.RemoteEndPoint)] = connection.RemoteEndPoint.ToString()
                    });
                }

                return array;
            }
        }
    }
}
