using System.Collections.Concurrent;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Sparrow.Collections;

namespace Raven.Server.TrafficWatch
{
    public class TrafficWatchManager
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(TrafficWatchManager));

        private static readonly ConcurrentSet<TrafficWatchConnection> ServerHttpTrace = new ConcurrentSet<TrafficWatchConnection>();

        public static bool IsRegisteredClients() => ServerHttpTrace.Count != 0;

        public static void AddConnection(TrafficWatchConnection connection)
        {
            ServerHttpTrace.Add(connection);
            Logger.Info($"TrafficWatch connection with Id={connection.Id} was opened");
        }

        public static void AddConnection(TrafficWatchConnection connection, string TenantName)
        {
            connection.TenantSpecific = TenantName;
            ServerHttpTrace.Add(connection);
            Logger.Info($"TrafficWatch connection with Id={connection.Id} was opened");
        }

        public static void Disconnect(TrafficWatchConnection connection)
        {
            if (ServerHttpTrace.TryRemove(connection) != true)
            {
                Logger.Error($"Couldn't remove connection of TrafficWatch with Id={connection.Id}");
                return;
            }
            Logger.Info($"TrafficWatch connection with Id={connection.Id} was closed");
        }

        public static void DispatchMessage(TrafficWatchNotification trafficWatchData)
        {
            foreach (var connection in ServerHttpTrace)
            {
                if (connection.TenantSpecific != null && trafficWatchData.TenantName != null &&
                    trafficWatchData.TenantName.Equals("db/" + connection.TenantSpecific) == false)
                    continue;

                connection.EnqueMsg(trafficWatchData);
            }
        }
    }
}