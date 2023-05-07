using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server;
using Tests.Infrastructure;

namespace FastTests;

public partial class RavenTestBase
{
    public class ReplicationManager : IReplicationManager
    {
        public readonly string DatabaseName;
        public readonly Dictionary<string, ReplicationInstance> Instances;

        public ReplicationManager(string databaseName, Dictionary<string, ReplicationInstance> instances)
        {
            DatabaseName = databaseName;
            Instances = instances;
        }

        public void Break()
        {
            foreach (var (node, replicationInstance) in Instances)
            {
                replicationInstance.Break();
            }
        }

        public void Mend()
        {
            foreach (var (node, replicationInstance) in Instances)
            {
                replicationInstance.Mend();
            }
        }

        public void ReplicateOnce(string docId)
        {
            foreach (var (node, replicationInstance) in Instances)
            {
                replicationInstance.ReplicateOnce(docId);
            }
        }

        public async Task EnsureNoReplicationLoopAsync()
        {
            foreach (var (node, replicationInstance) in Instances)
            {
                await replicationInstance.EnsureNoReplicationLoopAsync();
            }
        }

        public void Dispose()
        {
            foreach (var instance in Instances.Values)
            {
                instance.Dispose();
            }
        }

        internal static async ValueTask<ReplicationManager> GetReplicationManagerAsync(List<RavenServer> servers, string databaseName)
        {
            Dictionary<string, ReplicationInstance> instances = new();
            foreach (var server in servers)
            {
                var instance = await ReplicationInstance.GetReplicationInstanceAsync(server, databaseName);
                if (instance != null)
                    instances[server.ServerStore.NodeTag] = instance;
            }

            return new ReplicationManager(databaseName, instances);
        }
    }
}
