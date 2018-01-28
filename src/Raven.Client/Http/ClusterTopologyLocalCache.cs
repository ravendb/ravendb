using System;
using System.IO;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Client.Http
{
    internal static class ClusterTopologyLocalCache
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger("Client", typeof(ClusterTopologyLocalCache).FullName);

        public static void Clear(string serverHash)
        {
            try
            {
                var path = GetPath(serverHash);
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

        private static string GetPath(string serverHash)
        {
            return Path.Combine(AppContext.BaseDirectory, serverHash + ".raven-cluster-topology");
        }

        public static ClusterTopologyResponse TryLoad(string serverHash, JsonOperationContext context)
        {
            try
            {
                var path = GetPath(serverHash);
                if (File.Exists(path) == false)
                    return null;

                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
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

        public static void TrySaving(string serverHash, ClusterTopologyResponse clusterTopology, JsonOperationContext context)
        {
            try
            {
                var path = GetPath(serverHash);
                if (clusterTopology == null)
                {
                    Clear(serverHash);
                    return;
                }

                using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read))
                using (var writer = new BlittableJsonTextWriter(context, stream))
                {
                    var json = new DynamicJsonValue
                    {
                        [nameof(clusterTopology.Topology)] = clusterTopology.Topology.ToJson(),
                        [nameof(clusterTopology.Leader)] = clusterTopology.Leader,
                        [nameof(clusterTopology.NodeTag)] = clusterTopology.NodeTag
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
