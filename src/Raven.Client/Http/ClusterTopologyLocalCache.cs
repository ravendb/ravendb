using System;
using System.IO;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization;
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

        private static string GetPath(string topologyHash, DocumentConventions conventions)
        {
            return Path.Combine(conventions.TopologyCacheLocation, $"{topologyHash}.raven-cluster-topology");
        }

        public static ClusterTopologyResponse TryLoad(string topologyHash, DocumentConventions conventions, JsonOperationContext context)
        {
            try
            {
                if (conventions.DisableTopologyCache)
                    return null;

                var path = GetPath(topologyHash, conventions);
                if (File.Exists(path) == false)
                    return null;

                using (var stream = SafeFileStream.Create(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (var json = context.Read(stream, "raven-cluster-topology"))
                {
                    return JsonDeserializationClient.ClusterTopology(json);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not understand the persisted cluster topology", e);
                return null;
            }
        }

        public static void TrySaving(string topologyHash, ClusterTopologyResponse clusterTopology, DocumentConventions conventions, JsonOperationContext context)
        {
            try
            {
                if (conventions.DisableTopologyCache)
                    return;

                var path = GetPath(topologyHash, conventions);
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
                        [nameof(clusterTopology.Etag)] = clusterTopology.Etag,
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
