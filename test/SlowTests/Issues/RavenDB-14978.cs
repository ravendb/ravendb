using System;
using System.Threading.Tasks;
using FastTests;
using Nito.AsyncEx;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14978 : ClusterTestBase
    {
        public RavenDB_14978(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_setup_write_load_balancing()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var databaseName = GetDatabaseName();

            string context = "users/1";

            using var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
                Conventions = new DocumentConventions
                {
                    ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin,
                    WriteBalanceBehavior = WriteBalanceBehavior.ClientContextSelection,
                    WriteBalanceSessionContextSelector = db => context
                }
            }.Initialize();
            
            var amre = new AsyncManualResetEvent();
            
            var requestExecutor = store.GetRequestExecutor();
            requestExecutor.ClientConfigurationChanged += (sender, _) => amre.Set();
            
            var (index, _) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);
            await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(30));

            using (var s0 = store.OpenSession())
            {
                s0.Load<User>("test/1");
            }

            await amre.WaitAsync();

            int s1Ctx = -1;
            
            using (var s1 = store.OpenSession())
            {
                var sessionInfo = s1.Advanced.SessionInfo;
                s1Ctx = sessionInfo.SessionId.Value;
            }

            int s2Ctx = -1;
            using (var s2 = store.OpenSession())
            {
                var sessionInfo = s2.Advanced.SessionInfo;
                s2Ctx = sessionInfo.SessionId.Value;
            }
            
            Assert.Equal(s2Ctx, s1Ctx);

            context = "users/2";

            int s3Ctx = -1;
            using (var s3 = store.OpenSession())
            {
                var sessionInfo = s3.Advanced.SessionInfo;
                s3Ctx = sessionInfo.SessionId.Value;
            }
            
            Assert.NotEqual(s2Ctx, s3Ctx);


        }

        public class User
        {
            
        }
    }
}
