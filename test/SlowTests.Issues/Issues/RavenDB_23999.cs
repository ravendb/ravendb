using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23999 : ClusterTestBase
{
    public RavenDB_23999(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Proxy_Will_Respect_Accept_Encoding_Header()
    {
        var result = await CreateRaftCluster(2);
        var nodeTag = result.Nodes.Single(x => x != result.Leader).ServerStore.NodeTag;

        using (var httpClient = new HttpClient(new HttpClientHandler { AutomaticDecompression = DecompressionMethods.GZip }))
        {
            var response = await httpClient.GetAsync($"{result.Leader.WebUrl}/admin/stats?nodeTag={nodeTag}");

            Assert.True(response.IsSuccessStatusCode);

            var json = await response.Content.ReadAsStringAsync();
            JsonConvert.DeserializeObject<ServerStatistics>(json); // this throws
        }
    }
}
