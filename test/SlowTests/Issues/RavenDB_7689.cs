using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions.Security;
using Raven.Client.Server;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Server;
using Raven.Server.Config.Attributes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7689 : ClusterTestBase
    {
        [Fact(Skip = "We're going to remove the api-keys")]
        public async Task InvalidateTheAccessTokenCache()
        {
            var apiKey = new ApiKeyDefinition
            {
                Enabled = true,
                Secret = "secret",
            };

            var leader = await CreateRaftClusterAndGetLeader(2);
            var member = Servers.Except(new[] {leader}).Single();

            using (var store = GetDocumentStore(defaultServer: leader, 
                replicationFactor: 1, 
                modifyDatabaseRecord: record => { record.Topology = new DatabaseTopology {Members = new List<string> {member.ServerStore.NodeTag}}; }, 
                apiKey: "super/" + apiKey.Secret))
            {
                PutApiKey(leader, apiKey, store.Database, AccessMode.ReadOnly);
                Assert.Empty(leader.AccessTokenCache);
                Assert.Empty(member.AccessTokenCache);

                await Assert.ThrowsAsync<AuthorizationException>(async () => await store.Commands().PutAsync("users/1", null, new User {Name = "Fitzchak"}));

                PutApiKey(leader, apiKey, store.Database, AccessMode.ReadWrite);
                Assert.Empty(leader.AccessTokenCache);
                Assert.Empty(member.AccessTokenCache);

                await store.Commands().PutAsync("users/2", null, new User {Name = "Fitzchak"});
            }

            // TODO: Test also invalidation after deleting the api-key
        }

        private void PutApiKey(RavenServer leader, ApiKeyDefinition apiKey, string database, AccessMode accessMode)
        {
            leader.Configuration.Security.AuthenticationEnabled = false;

            using (var store = GetDocumentStore(defaultServer: leader,
                createDatabase: false,
                apiKey: "super/" + apiKey.Secret))
            {
                apiKey.ResourcesAccessMode[database] = accessMode;
                store.Admin.Server.Send(new PutApiKeyOperation("super", apiKey));
            }

            leader.Configuration.Security.AuthenticationEnabled = true;
        }
    }
}
