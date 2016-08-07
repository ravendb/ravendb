using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Data;
using Raven.Client.Documents.Commands;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Http
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
                       return JsonDeserialization.ClusterTopology(blittableJsonReaderObject);
                    }
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not understand the persisted replication information", e);
                return null;
            }
        }

        public static void TrySavingTopologyToLocalCache(string serverHash, JsonDocument document)
        {
            try
            {
                var path = GetTopologyPath(serverHash);
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
    }
}
