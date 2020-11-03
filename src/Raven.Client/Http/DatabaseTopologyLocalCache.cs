using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Client.Http
{
    internal static class DatabaseTopologyLocalCache
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger("Client", typeof(DatabaseTopologyLocalCache).FullName);

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
                    _logger.Info("Could not clear the persisted database topology", e);
            }
        }

        private static string GetPath(string databaseName, string topologyHash, DocumentConventions conventions)
        {
            return Path.Combine(conventions.TopologyCacheLocation, $"{databaseName}.{topologyHash}.raven-database-topology");
        }

        public static Task<Topology> TryLoadAsync(string databaseName, string topologyHash, DocumentConventions conventions, JsonOperationContext context)
        {
            if (conventions.DisableTopologyCache)
                return null;

            var path = GetPath(databaseName, topologyHash, conventions);
            return TryLoadAsync(path, context);
        }

        private static async Task<Topology> TryLoadAsync(string path, JsonOperationContext context)
        {
            try
            {
                if (File.Exists(path) == false)
                    return null;

                using (var stream = SafeFileStream.Create(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (var json = await context.ReadForMemoryAsync(stream, "raven-database-topology").ConfigureAwait(false))
                {
                    return JsonDeserializationClient.Topology(json);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not understand the persisted database topology", e);
                return null;
            }
        }

        public static async Task TrySavingAsync(string databaseName, string topologyHash, Topology topology, DocumentConventions conventions, JsonOperationContext context, CancellationToken token)
        {
            try
            {
                if (conventions.DisableTopologyCache)
                    return;

                var path = GetPath(databaseName, topologyHash, conventions);
                if (topology == null)
                {
                    Clear(path);
                    return;
                }

                var existingTopology = await TryLoadAsync(path, context).ConfigureAwait(false);
                if (existingTopology?.Etag >= topology.Etag)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Skipping save topology with etag {topology.Etag} to cache " +
                                     $"as the cache already have a topology with etag: {existingTopology.Etag}");
                    return;
                }

                using (var stream = SafeFileStream.Create(path, FileMode.Create, FileAccess.Write, FileShare.Read))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(context.GetLazyString(nameof(topology.Nodes)));
                    writer.WriteStartArray();
                    for (var i = 0; i < topology.Nodes.Count; i++)
                    {
                        var node = topology.Nodes[i];
                        if (i != 0)
                            writer.WriteComma();
                        WriteNode(writer, node, context);
                    }
                    writer.WriteEndArray();

                    writer.WriteComma();
                    writer.WritePropertyName(context.GetLazyString(nameof(topology.Etag)));
                    writer.WriteInteger(topology.Etag);

                    writer.WriteComma();
                    writer.WritePropertyName(context.GetLazyString("PersistedAt"));
                    writer.WriteString(DateTimeOffset.UtcNow.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite));

                    writer.WriteEndObject();
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not persist the database topology", e);
            }
        }

        private static void WriteNode(AsyncBlittableJsonTextWriter writer, ServerNode node, JsonOperationContext context)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(ServerNode.Url)));
            writer.WriteString(context.GetLazyString(node.Url));

            writer.WriteComma();
            writer.WritePropertyName(context.GetLazyString(nameof(ServerNode.Database)));
            writer.WriteString(context.GetLazyString(node.Database));

            // ClusterTag and ServerRole included for debugging purpose only
            writer.WriteComma();
            writer.WritePropertyName(context.GetLazyString(nameof(ServerNode.ClusterTag)));
            writer.WriteString(context.GetLazyString(node.ClusterTag));

            writer.WriteComma();
            writer.WritePropertyName(context.GetLazyString(nameof(ServerNode.ServerRole)));
            writer.WriteString(context.GetLazyString(node.ServerRole.ToString()));

            writer.WriteEndObject();
        }
    }
}
