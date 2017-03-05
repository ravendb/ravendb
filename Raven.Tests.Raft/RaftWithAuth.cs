// -----------------------------------------------------------------------
//  <copyright file="RaftWithAuth.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Rachis.Transport;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using Raven.Database.Raft;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Database.Server.Security;

using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Raft
{
    public class RaftWithAuth : RaftTestBase
    {
        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            Authentication.EnableOnce();
        }

        [Fact]
        public async Task CanCreateClusterWithApiKeyAndSendCommandToLeader()
        {
            NodeConnectionInfo leaderNci;
            var leader = CreateServerWithOAuth(8079, "Ayende/abc", out leaderNci);

            leader.Options.ClusterManager.Value.InitializeTopology(leaderNci,forceCandidateState:true);
            Assert.True(leader.Options.ClusterManager.Value.Engine.WaitForLeader());

            NodeConnectionInfo secondConnectionInfo;
            CreateServerWithOAuth(8078, "Marcin/cba", out secondConnectionInfo);

            Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(secondConnectionInfo).Wait(3000));
            Assert.True(leader.Options.ClusterManager.Value.Engine.WaitForLeader());
            //If we don't wait here we can cause two topology changes to happen at the same time and fail.

            WaitForClusterToBecomeNonStale(2);
            NodeConnectionInfo thirdConnectionInfo;
            CreateServerWithOAuth(8077, "User3/pass", out thirdConnectionInfo);
            Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(thirdConnectionInfo).Wait(3000));

            Assert.True(servers[0].Options.ClusterManager.Value.IsLeader());
            var client = servers[0].Options.ClusterManager.Value.Client;
            await client.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = true });

            Assert.Equal(3, stores.Count);

            stores.ForEach(store =>
            {
                WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey, TimeSpan.FromMinutes(1));
                var configurationJson = store.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
                var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
                Assert.True(configuration.EnableReplication);
            });
        }

        [Fact(Skip = "Windows credentials required.")]
        public async Task CanCreateClusterWithWindowsAuthAndSendCommandToLeader()
        {
            const string username = "";
            const string password = "";
            const string domain = "";

            Assert.NotNull(password);

            NodeConnectionInfo leaderNci;
            var leader = CreateServerWithWindowsCredentials(8079, username, password, domain, out leaderNci);

            leader.Options.ClusterManager.Value.InitializeTopology(leaderNci,forceCandidateState: true);
            Assert.True(leader.Options.ClusterManager.Value.Engine.WaitForLeader());

            NodeConnectionInfo secondConnectionInfo;
            CreateServerWithWindowsCredentials(8078, username, password, domain, out secondConnectionInfo);
            Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(secondConnectionInfo).Wait(3000));
            //If we don't wait here we can cause two topology changes to happen at the same time and fail.
            WaitForClusterToBecomeNonStale(2);
            NodeConnectionInfo thirdConnectionInfo;
            CreateServerWithWindowsCredentials(8077, username, password, domain, out thirdConnectionInfo);
            Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(thirdConnectionInfo).Wait(3000));

            Assert.True(servers[0].Options.ClusterManager.Value.IsLeader());
            var client = servers[0].Options.ClusterManager.Value.Client;
            await client.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = true });

            Assert.Equal(3, stores.Count);

            stores.ForEach(store =>
            {
                WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey, TimeSpan.FromMinutes(1));
                var configurationJson = store.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
                var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
                Assert.True(configuration.EnableReplication);
            });
        }

        protected override void ModifyStore(DocumentStore documentStore)
        {
            documentStore.Credentials = null;
        }

        [Fact]
        public async Task CanCreateClusterWithApiKeyAndSendCommandToNonLeader()
        {
            NodeConnectionInfo leaderNci;
            var leader = CreateServerWithOAuth(8079, "Ayende/abc", out leaderNci);

            leader.Options.ClusterManager.Value.InitializeTopology(leaderNci, forceCandidateState: true);
            Assert.True(leader.Options.ClusterManager.Value.Engine.WaitForLeader());

            NodeConnectionInfo secondConnectionInfo;
            CreateServerWithOAuth(8078, "Marcin/cba", out secondConnectionInfo);
            Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(secondConnectionInfo).Wait(3000));
            //If we don't wait here we can cause two topology changes to happen at the same time and fail.
            WaitForClusterToBecomeNonStale(2);

            NodeConnectionInfo thirdConnectionInfo;
            CreateServerWithOAuth(8077, "User3/pass", out thirdConnectionInfo);
            Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(thirdConnectionInfo).Wait(3000));

            WaitForClusterToBecomeNonStale(3);

            Assert.False(servers[1].Options.ClusterManager.Value.IsLeader());
            var client = servers[1].Options.ClusterManager.Value.Client;
            await client.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = false });

            Assert.Equal(3, stores.Count);

            stores.ForEach(store =>
            {
                WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey, TimeSpan.FromMinutes(1));
                var configurationJson = store.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
                var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
                Assert.False(configuration.EnableReplication);
            });
        }

        [Fact(Skip = "Windows credentials required.")]
        public async Task CanCreateClusterWithWindowsAuthAndSendCommandToNonLeader()
        {
            const string username = "";
            const string password = "";
            const string domain = "";

            Assert.NotNull(password);

            NodeConnectionInfo leaderNci;
            var leader = CreateServerWithWindowsCredentials(8079, username, password, domain, out leaderNci);

            leader.Options.ClusterManager.Value.InitializeTopology(leaderNci, forceCandidateState: true);
            Assert.True(leader.Options.ClusterManager.Value.Engine.WaitForLeader());

            NodeConnectionInfo secondConnectionInfo;
            CreateServerWithWindowsCredentials(8078, username, password, domain, out secondConnectionInfo);
            Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(secondConnectionInfo).Wait(3000));
            //If we don't wait here we can cause two topology changes to happen at the same time and fail.
            WaitForClusterToBecomeNonStale(2);
            NodeConnectionInfo thirdConnectionInfo;
            CreateServerWithWindowsCredentials(8077, username, password, domain, out thirdConnectionInfo);
            Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(thirdConnectionInfo).Wait(3000));

            WaitForClusterToBecomeNonStale(3);

            Assert.False(servers[1].Options.ClusterManager.Value.IsLeader());
            var client = servers[1].Options.ClusterManager.Value.Client;
            await client.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = false });

            Assert.Equal(3, stores.Count);

            stores.ForEach(store =>
            {
                WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey, TimeSpan.FromMinutes(1));
                var configurationJson = store.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
                var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
                Assert.False(configuration.EnableReplication);
            });
        }
    }
}
