using System;
using Raven.Abstractions.Data;
using Sparrow.Collections;

namespace Raven.Server.TrafficWatch
{
    public class TrafficWatchManager
    {
        private static readonly ConcurrentSet<TrafficWatchConnection> ServerHttpTrace = new ConcurrentSet<TrafficWatchConnection>();
        
        public static bool HasRegisteredClients => ServerHttpTrace.Count != 0;

        public static void AddConnection(TrafficWatchConnection connection)
        {
            ServerHttpTrace.Add(connection);
        }

        public static void Disconnect(TrafficWatchConnection connection)
        {
            ServerHttpTrace.TryRemove(connection);
        }

        public static void DispatchMessage(TrafficWatchNotification trafficWatchData)
        {
            foreach (var connection in ServerHttpTrace)
            {
                if (connection.IsAlive == false)
                {
                    ServerHttpTrace.TryRemove(connection);
                    continue;
                }

                if (connection.TenantSpecific != null)
                {
                    if (string.Equals(connection.TenantSpecific, trafficWatchData.TenantName, StringComparison.OrdinalIgnoreCase) == false)
                        continue;
                }
                connection.EnqueMsg(trafficWatchData);
            }
        }
    }
}