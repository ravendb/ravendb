using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Extensions;

namespace Raven.Server.Web.System
{
    public class AdminTcpConnectionDebugInfoHandler : RequestHandler
    {
        [RavenAction("/admin/debug/info/tcp/stats", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task Statistics()
        {
            var properties = TcpExtensions.GetIPGlobalPropertiesSafely();
            var ipv4Stats = properties.GetTcpIPv4StatisticsSafely();
            var ipv6Stats = properties.GetTcpIPv6StatisticsSafely();

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
                if (stats == null)
                    return null;

                var result = new DynamicJsonValue();
                result[nameof(stats.ConnectionsAccepted)] = stats.GetConnectionsAcceptedSafely();
                result[nameof(stats.ConnectionsInitiated)] = stats.GetConnectionsInitiatedSafely();
                result[nameof(stats.CumulativeConnections)] = stats.GetCumulativeConnectionsSafely();
                result[nameof(stats.CurrentConnections)] = stats.GetCurrentConnectionsSafely();
                result[nameof(stats.ErrorsReceived)] = stats.GetErrorsReceivedSafely();
                result[nameof(stats.FailedConnectionAttempts)] = stats.GetFailedConnectionAttemptsSafely();
                result[nameof(stats.MaximumConnections)] = stats.GetMaximumConnectionsSafely();
                result[nameof(stats.MaximumTransmissionTimeout)] = stats.GetMaximumTransmissionTimeoutSafely();
                result[nameof(stats.MinimumTransmissionTimeout)] = stats.GetMinimumTransmissionTimeoutSafely();
                result[nameof(stats.ResetConnections)] = stats.GetResetConnectionsSafely();
                result[nameof(stats.ResetsSent)] = stats.GetResetsSentSafely();
                result[nameof(stats.SegmentsReceived)] = stats.GetSegmentsReceivedSafely();
                result[nameof(stats.SegmentsResent)] = stats.GetSegmentsResentSafely();
                result[nameof(stats.SegmentsSent)] = stats.GetSegmentsSentSafely();

                return result;
            }
        }

        [RavenAction("/admin/debug/info/tcp/active-connections", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public Task ActiveConnections()
        {
            var properties = TcpExtensions.GetIPGlobalPropertiesSafely();
            var connections = properties.GetActiveTcpConnectionsSafely();

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
                if (connections == null || connections.Length == 0)
                    return null;

                var result = new DynamicJsonValue();

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
