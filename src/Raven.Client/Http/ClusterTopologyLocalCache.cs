using System;
using System.IO;
using Raven.Client.Json.Converters;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Client.Http
{
    internal static class ClusterTopologyLocalCache
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger("Client", typeof(ClusterTopologyLocalCache).FullName);

        private static void Clear(string path)
        {
            try
            {
                if (File.Exists(path) == false)
                    return;

                File.Delete(path);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not clear the persisted cluster topology", e);
            }
        }

        private static string GetPath(string topologyHash)
        {
            return Path.Combine(AppContext.BaseDirectory, topologyHash + ".raven-cluster-topology");
        }

        public static ClusterTopologyResponse TryLoad(string topologyHash, JsonOperationContext context)
        {
            try
            {
                var path = GetPath(topologyHash);
                if (File.Exists(path) == false)
                    return null;

                using (var stream = SafeFileStream.Create(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (var blittableJsonReaderObject = context.Read(stream, "raven-cluster-topology"))
                {
                    return JsonDeserializationClient.ClusterTopology(blittableJsonReaderObject);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not understand the persisted cluster topology", e);
                return null;
            }
        }

        public static void TrySaving(string topologyHash, ClusterTopologyResponse clusterTopology, JsonOperationContext context)
        {
            try
            {
                var path = GetPath(topologyHash);
                if (clusterTopology == null)
                {
                    Clear(path);
                    return;
                }

                using (var stream = SafeFileStream.Create(path, FileMode.Create, FileAccess.Write, FileShare.Read))
                using (var writer = new BlittableJsonTextWriter(context, stream))
                {
                    var json = new DynamicJsonValue
                    {
                        [nameof(clusterTopology.Topology)] = clusterTopology.Topology.ToJson(),
                        [nameof(clusterTopology.Leader)] = clusterTopology.Leader,
                        [nameof(clusterTopology.NodeTag)] = clusterTopology.NodeTag,
                        ["PersistedAt"] = DateTimeOffset.UtcNow.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite),
                    };

                    context.Write(writer, json);
                    writer.Flush();
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not persist the cluster topology", e);
            }
        }
    }
}
