using System;
using System.IO;
using Raven.NewClient.Abstractions.Logging;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Http
{
    public static class TopologyLocalCache
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(TopologyLocalCache));

        public static void ClearTopologyFromLocalCache(string serverHash)
        {
            try
            {
                var path = GetTopologyPath(serverHash);

                if (File.Exists(path) == false)
                    return;

                File.Delete(path);
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not clear the persisted replication information", e);
            }
        }

        private static string GetTopologyPath(string serverHash)
        {
            return Path.Combine(AppContext.BaseDirectory, serverHash + ".raven-topology");
        }

        public static Topology TryLoadTopologyFromLocalCache(string serverHash, JsonOperationContext context)
        {
            try
            {
                var path = GetTopologyPath(serverHash);
                if (File.Exists(path) == false)
                    return null;

                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    using (var blittableJsonReaderObject = context.Read(stream, "raven-topology"))
                    {
                       return JsonDeserializationClient.ClusterTopology(blittableJsonReaderObject);
                    }
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not understand the persisted replication information", e);
                return null;
            }
        }

        public static void TrySavingTopologyToLocalCache(string serverHash, Topology topology, JsonOperationContext context)
        {
            try
            {
                var path = GetTopologyPath(serverHash);
                if (topology == null)
                {
                    ClearTopologyFromLocalCache(serverHash);
                    return;
                }

                using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read))
                using (var writer = new BlittableJsonTextWriter(context, stream))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(context.GetLazyString(nameof(Topology.LeaderNode)));
                    WriteNode(writer, topology.LeaderNode, context);

                    writer.WritePropertyName(context.GetLazyString(nameof(Topology.Nodes)));
                    writer.WriteStartArray();
                    foreach (var node in topology.Nodes)
                    {
                        WriteNode(writer, node, context);
                    }
                    writer.WriteEndArray();

                    writer.WritePropertyName(context.GetLazyString(nameof(Topology.ReadBehavior)));
                    writer.WriteString(context.GetLazyString(topology.ReadBehavior.ToString()));

                    writer.WritePropertyName(context.GetLazyString(nameof(Topology.WriteBehavior)));
                    writer.WriteString(context.GetLazyString(topology.WriteBehavior.ToString()));
                    writer.WriteEndObject();
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not persist the replication information", e);
            }
        }

        private static void WriteNode(BlittableJsonTextWriter writer, ServerNode node, JsonOperationContext context)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(context.GetLazyString(nameof(ServerNode.Url)));
            writer.WriteString(context.GetLazyString(node.Url));
            writer.WritePropertyName(context.GetLazyString(nameof(ServerNode.Database)));
            writer.WriteString(context.GetLazyString(node.Database));
            writer.WriteEndObject();
        }
    }
}
