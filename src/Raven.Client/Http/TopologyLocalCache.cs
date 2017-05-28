using System;
using System.IO;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Http
{
    internal static class TopologyLocalCache
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger("Client", typeof(TopologyLocalCache).FullName);

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
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Could not clear the persisted replication information", e);
                }
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
                        return JsonDeserializationClient.Topology(blittableJsonReaderObject);
                    }
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Could not understand the persisted replication information", e);
                }
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
                    writer.WritePropertyName(context.GetLazyString(nameof(Topology.Nodes)));
                    writer.WriteStartArray();
                    for (var index = 0; index < topology.Nodes.Count; index++)
                    {
                        var node = topology.Nodes[index];
                        if(index != 0)
                            writer.WriteComma();
                        WriteNode(writer, node, context);
                    }
                    writer.WriteEndArray();
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(Topology.ReadBehavior)));
                    writer.WriteString(context.GetLazyString(topology.ReadBehavior.ToString()));
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(Topology.WriteBehavior)));
                    writer.WriteString(context.GetLazyString(topology.WriteBehavior.ToString()));
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(Topology.SLA)));
                    writer.WriteStartObject();
                    writer.WritePropertyName(context.GetLazyString(nameof(topology.SLA.RequestTimeThresholdInMilliseconds)));
                    writer.WriteInteger(topology.SLA.RequestTimeThresholdInMilliseconds);
                    writer.WriteEndObject();

                    writer.WriteEndObject();
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Could not persist the replication information", e);
                }

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
