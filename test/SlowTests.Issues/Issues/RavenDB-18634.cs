using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_18634 : ClusterTestBase
    {
        public RavenDB_18634(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Cluster | RavenTestCategory.Configuration)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DisableTcpCompressionIn1ServerOutOf2InCluster(bool watcherCluster)
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: watcherCluster);

            // modify configuration
            ExecuteScript(leader, database: null, "server.Configuration.Server.DisableTcpCompression = true;");
            Assert.True(leader.Configuration.Server.DisableTcpCompression);

            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2});

            var db0 = await nodes[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            var db1 = await nodes[1].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            Assert.NotNull(db0);
            Assert.NotNull(db1);
        }
        
        private JToken ExecuteScript(DocumentDatabase database, string script)
        {
            return ExecuteScript(Server, database, script);
        }

        internal static JToken ExecuteScript(RavenServer server, DocumentDatabase database, string script)
        {
            var result = new AdminJsConsole(server, database).ApplyScript(new AdminJsScript
            (
                script
            ));

            Assert.NotNull(result);
            var token = JsonConvert.DeserializeObject<JObject>(result).GetValue("Result");
            Assert.NotNull(token);
            return token;
        }
    }
}
