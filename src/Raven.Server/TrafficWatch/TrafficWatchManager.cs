using System;
using Raven.Client.Documents.Changes;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Logging;

namespace Raven.Server.TrafficWatch
{
    internal static class TrafficWatchManager
    {
        private static readonly ConcurrentSet<TrafficWatchConnection> ServerHttpTrace = new ConcurrentSet<TrafficWatchConnection>();

        public static bool EnableTrafficWatchToLog;

        public static bool HasRegisteredClients => ServerHttpTrace.Count != 0 || EnableTrafficWatchToLog;

        public static void AddConnection(TrafficWatchConnection connection)
        {
            ServerHttpTrace.Add(connection);
        }

        public static void Disconnect(TrafficWatchConnection connection)
        {
            ServerHttpTrace.TryRemove(connection);
            connection.Dispose();
        }

        public static void DispatchMessage(TrafficWatchChangeBase trafficWatchData, Logger logger)
        {
            foreach (var connection in ServerHttpTrace)
            {
                if (connection.IsAlive == false)
                {
                    Disconnect(connection);
                    continue;
                }

                if (connection.TenantSpecific != null)
                {
                    if (string.Equals(connection.TenantSpecific, trafficWatchData.DatabaseName, StringComparison.OrdinalIgnoreCase) == false)
                        continue;
                }

                connection.EnqueueMsg(trafficWatchData);
            }

            if (logger != null && logger.IsOperationsEnabled && EnableTrafficWatchToLog)
            {
                var json = trafficWatchData.ToJson();
                string msg = string.Empty;
                var first = true;
                foreach (var (name, value) in json.Properties)
                {
                    if (first == false)
                        msg += ", ";

                    first = false;

                    if (name == nameof(TrafficWatchHttpChange.ResponseSizeInBytes))
                    {
                        msg += new Size((long)value, SizeUnit.Bytes).ToString();
                    }
                    else
                    {
                        msg += value ?? "N/A";
                    }
                }

                logger.Operations(msg);
            }
        }
    }
}
