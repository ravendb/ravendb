using System;
using System.Collections.Generic;
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

        public static void ClearTopologyFromLocalCache(string serverHash)
        {
            try
            {
                var path = GetClusterTopologyPath(serverHash);

                if (File.Exists(path) == false)
                    return;

                File.Delete(path);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Could not clear the persisted cluster replication information", e);
                }
            }
        }

        private static string GetClusterTopologyPath(string serverHash)
        {
            return Path.Combine(AppContext.BaseDirectory, serverHash + ".raven-cluster-topology");
        }

        public static ClusterTopologyResponse TryLoadClusterTopologyFromLocalCache(string serverHash, JsonOperationContext context)
        {
            try
            {
                var path = GetClusterTopologyPath(serverHash);
                if (File.Exists(path) == false)
                    return null;

                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    using (var blittableJsonReaderObject = context.Read(stream, "raven-cluster-topology"))
                    {
                        return JsonDeserializationClient.ClusterTopology(blittableJsonReaderObject);
                    }
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Could not understand the persisted cluster replication information", e);
                }
                return null;
            }
        }

        public static void TrySavingTopologyToLocalCache(string serverHash, ClusterTopologyResponse clusterTopology, JsonOperationContext context)
        {
            try
            {
                var path = GetClusterTopologyPath(serverHash);
                if (clusterTopology == null)
                {
                    ClearTopologyFromLocalCache(serverHash);
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
                {
                    _logger.Info("Could not persist the replication information", e);
                }

            }
        }
    }
}
