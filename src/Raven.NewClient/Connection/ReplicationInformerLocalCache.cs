using System;
using System.Collections.Generic;
using System.IO;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Logging;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Connection
{
    public static class ReplicationInformerLocalCache
    {
        private readonly static ILog Log = LogManager.GetLogger(typeof(ReplicationInformerLocalCache));

        public static void ClearReplicationInformationFromLocalCache(string serverHash)
        {
            try
            {
                var path = GetDocsReplicationInformerPath(serverHash);

                if (File.Exists(path) == false)
                    return;

                File.Delete(path);
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not clear the persisted replication information", e);
            }
        }

        private static string GetDocsReplicationInformerPath(string serverHash)
        {
            return Path.Combine(AppContext.BaseDirectory, serverHash + ".raven-docs-replication-info");
        }


        private static string GetClusterReplicationPath(string serverHash)
        {
            return Path.Combine(AppContext.BaseDirectory, serverHash + ".raven-cluster-replication-info");

        }

        public static JsonDocument TryLoadReplicationInformationFromLocalCache(string serverHash)
        {
            try
            {
                var path = GetDocsReplicationInformerPath(serverHash);
                if (File.Exists(path) == false)
                    return null;

                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    return stream.ToJObject().ToJsonDocument();
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not understand the persisted replication information", e);
                return null;
            }
        }

        public static void TrySavingReplicationInformationToLocalCache(string serverHash, JsonDocument document)
        {
            try
            {
                var path = GetDocsReplicationInformerPath(serverHash);
                using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read))
                {
                    document.ToJson().WriteTo(stream);
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not persist the replication information", e);
            }
        }

        public static List<OperationMetadata> TryLoadClusterNodesFromLocalCache(string serverHash)
        {
            try
            {
                var path = GetClusterReplicationPath(serverHash);

                if (File.Exists(path) == false)
                    return null;

                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    return RavenJToken
                        .TryLoad(stream)
                        .JsonDeserialization<List<OperationMetadata>>();
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not understand the persisted cluster nodes", e);
                return null;
            }
        }

        public static void TrySavingClusterNodesToLocalCache(string serverHash, List<OperationMetadata> nodes)
        {
            try
            {
                var path = GetClusterReplicationPath(serverHash);

                using (var stream = new FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read))
                {
                    RavenJToken.FromObject(nodes).WriteTo(stream);
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not persist the cluster nodes", e);
            }
        }
    }
}
