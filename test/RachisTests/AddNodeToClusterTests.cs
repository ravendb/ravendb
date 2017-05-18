using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{
    public class AddNodeToClusterTests : ClusterTestBase
    {
        [Fact]
        public async Task FailOnAddingNonPassiveNode()
        {
            NoTimeouts();
            var raft1 = await CreateRaftClusterAndGetLeader(1);
            var raft2 = await CreateRaftClusterAndGetLeader(1);
            
            var url = raft2.WebUrls[0];
            await raft1.ServerStore.AddNodeToClusterAsync(url);
            Assert.False(await raft1.ServerStore.WaitForNodeBecomeMember(url));
        }
    }
}
