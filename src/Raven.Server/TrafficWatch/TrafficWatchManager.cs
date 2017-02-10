using System;
using Raven.Client.Data;
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
            connection.Dispose();
        }

        public static void DispatchMessage(TrafficWatchChange trafficWatchData)
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
                    if (string.Equals(connection.TenantSpecific, trafficWatchData.TenantName, StringComparison.OrdinalIgnoreCase) == false)
                        continue;
                }
                connection.EnqueMsg(trafficWatchData);
            }
        }
    }
}