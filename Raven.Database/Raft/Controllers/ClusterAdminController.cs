// -----------------------------------------------------------------------
//  <copyright file="ClusterAdminController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Rachis;
using Rachis.Transport;
using Rachis.Utils;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Raft.Controllers
{
    public class ClusterAdminController : BaseAdminDatabaseApiController
    {
        [HttpPut]
        [RavenRoute("admin/cluster/commands/configuration")]
        public async Task<HttpResponseMessage> ClusterConfiguration()
        {
            var configuration = await ReadJsonObjectAsync<ClusterConfiguration>().ConfigureAwait(false);
            if (configuration == null)
                return GetEmptyMessage(HttpStatusCode.BadRequest);
            //Changing the replication check state is something that the admin will do when he has no leader.
            //But if we have no leader we can't run this command through raft because it will fail...
            //for this case i'll check that all but the replication state check are the same and if so
            //i'll apply the change localy to this server, allowing it to become leader.
            try
            {
                await ClusterManager.Client.SendClusterConfigurationAsync(configuration).ConfigureAwait(false);
            }
            catch
            {
                var configurationJson = SystemDatabase.Documents.Get(Constants.Cluster.ClusterConfigurationDocumentKey, null);
                var localConfiguration = configurationJson?.DataAsJson.JsonDeserialization<ClusterConfiguration>();
                //This should not be the case but i don't want NRE hiding the real problem.
                if (localConfiguration == null)
                    throw;
                if (!ConfigurationsAreEqualExceptReplicationCheck(configuration, localConfiguration)) throw;
                SystemDatabase.Documents.Put(Constants.Cluster.ClusterConfigurationDocumentKey, null, RavenJObject.FromObject(configuration), new RavenJObject(), null);
                return GetEmptyMessage();
            }
            return GetEmptyMessage();
        }

        private bool ConfigurationsAreEqualExceptReplicationCheck(ClusterConfiguration configuration, ClusterConfiguration localConfiguration)
        {
            if(configuration.EnableReplication != localConfiguration.EnableReplication)
                return false;
            if (configuration.DatabaseSettings == null && localConfiguration.DatabaseSettings == null)
                return true;
            if (configuration.DatabaseSettings == null || localConfiguration.DatabaseSettings == null)
                return false;
            if (configuration.DatabaseSettings.Keys.Count != localConfiguration.DatabaseSettings.Keys.Count)
                return false;
            foreach (var keyValue in configuration.DatabaseSettings)
            {
                string val;
                if (!localConfiguration.DatabaseSettings.TryGetValue(keyValue.Key, out val) || keyValue.Value != val)
                    return false;
            }
            return true;
        }

        [HttpPut]
        [RavenRoute("admin/cluster/commands/database/{*id}")]
        public async Task<HttpResponseMessage> CreateDatabase(string id)
        {
            var document = await ReadJsonObjectAsync<DatabaseDocument>().ConfigureAwait(false);
            if (document == null)
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            if (document.IsClusterDatabase() == false)
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            await ClusterManager.Client.SendDatabaseUpdateAsync(id, document).ConfigureAwait(false);
            return GetEmptyMessage();
        }

        [HttpDelete]
        [RavenRoute("admin/cluster/commands/database/{*id}")]
        public async Task<HttpResponseMessage> DeleteDatabase(string id)
        {
            bool result;
            var hardDelete = bool.TryParse(GetQueryStringValue("hard-delete"), out result) && result;

            if (string.IsNullOrEmpty(id))
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            var documentJson = SystemDatabase.Documents.Get(DatabaseHelper.GetDatabaseKey(id), null);
            if (documentJson == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            var document = documentJson.DataAsJson.JsonDeserialization<DatabaseDocument>();
            if (document.IsClusterDatabase() == false)
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            await ClusterManager.Client.SendDatabaseDeleteAsync(id, hardDelete).ConfigureAwait(false);
            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("admin/cluster/create")]
        public async Task<HttpResponseMessage> Create()
        {
            var topology = ClusterManager.Engine.CurrentTopology;

            if (ClusterManager.IsLeader())
                return GetEmptyMessage(HttpStatusCode.NotModified);

            if (topology.AllNodes.Any())
                return GetMessageWithString("Server is already in cluster.", HttpStatusCode.NotAcceptable);

            int nextStart = 0;
            var databases = SystemDatabase
                .Documents
                .GetDocumentsWithIdStartingWith(Constants.Database.Prefix, null, null, 0, int.MaxValue, CancellationToken.None, ref nextStart);
            
            if (databases.Length > 0)
                return GetMessageWithString("To create a cluster server must not contain any databases.", HttpStatusCode.NotAcceptable);

            var nodeConnectionInfo = await ReadJsonObjectAsync<NodeConnectionInfo>().ConfigureAwait(false);
            nodeConnectionInfo.Name = ClusterManager.Engine.Name;

            ClusterManager.InitializeTopology(nodeConnectionInfo);
            ClusterManager.Engine.WaitForLeader();
            return GetEmptyMessage(HttpStatusCode.Created);
        }

        [HttpPatch]
        [RavenRoute("admin/cluster/initialize-new-cluster")]
        public HttpResponseMessage InitializeNewCluster()
        {
            var oldClusterManager = SetClusterManagerToNullAndGetOldValue();
            oldClusterManager?.Dispose();

            var newClusterManager = ClusterManagerFactory.Create(SystemDatabase, DatabasesLandlord, nullifyLastAppliedIndex: true);
            newClusterManager.InitializeTopology(isPartOfExistingCluster: true);

            ((Reference<ClusterManager>) Configuration.Properties[typeof(ClusterManager)]).Value = newClusterManager;
            return GetEmptyMessage(HttpStatusCode.NoContent);
        }

        private ClusterManager SetClusterManagerToNullAndGetOldValue()
        {
            //making sure nobody contact us while we change the persistent state.
            ClusterManager oldClusterManager = ((Reference<ClusterManager>) Configuration.Properties[typeof(ClusterManager)]).Value;
            ((Reference<ClusterManager>) Configuration.Properties[typeof(ClusterManager)]).Value = null;
            return oldClusterManager;
        }

        [HttpPatch]
        [RavenRoute("admin/cluster/remove-clustering")]
        public HttpResponseMessage RemoveClustering()
        {
            if (ClusterManager.Engine.CurrentTopology.AllNodes.Count() > 1)
            {
                return GetMessageWithString("Remove clustering is available on single node clusters only.", HttpStatusCode.BadRequest);
            }
            var oldClusterManager = SetClusterManagerToNullAndGetOldValue();
            // delete Raft persistent storage and init new one
            oldClusterManager.CleanupAllClusteringData(SystemDatabase);

            var newClusterManager = ClusterManagerFactory.Create(SystemDatabase, DatabasesLandlord);
            ((Reference<ClusterManager>) Configuration.Properties[typeof(ClusterManager)]).Value = newClusterManager;

            return GetEmptyMessage(HttpStatusCode.NoContent);
        }

        [HttpPost]
        [RavenRoute("admin/cluster/join")]
        public async Task<HttpResponseMessage> JoinToCluster()
        {
            var nodeConnectionInfo = await ReadJsonObjectAsync<NodeConnectionInfo>().ConfigureAwait(false);
            if (nodeConnectionInfo == null)
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            if (nodeConnectionInfo.Name == null)
                nodeConnectionInfo.Name = RaftHelper.GetNodeName(await ClusterManager.Client.GetDatabaseId(nodeConnectionInfo).ConfigureAwait(false));

            var topology = ClusterManager.Engine.CurrentTopology;
            
            var canJoinResult = await ClusterManager.Client.SendCanJoinAsync(nodeConnectionInfo).ConfigureAwait(false);
            switch (canJoinResult)
            {
                case CanJoinResult.IsNonEmpty:
                    return GetMessageWithString("Can't join node to cluster. Node is not empty", HttpStatusCode.BadRequest);
                case CanJoinResult.InAnotherCluster:
                    return GetMessageWithString("Can't join node to cluster. Node is in different cluster", HttpStatusCode.BadRequest);
                case CanJoinResult.AlreadyJoined:
                    return GetEmptyMessage(HttpStatusCode.NotModified);
            }

            if (topology.Contains(nodeConnectionInfo.Name))
                return GetEmptyMessage(HttpStatusCode.NotModified);
            //overriding user request since we know that this node can only join as non voter            
            if (canJoinResult == CanJoinResult.CanJoinAsNonVoter)
                nodeConnectionInfo.IsNoneVoter = true;
            await ClusterManager.Client.SendJoinServerAsync(nodeConnectionInfo).ConfigureAwait(false);
            return GetEmptyMessage();
        }


        [HttpPost]
        [RavenRoute("admin/cluster/update")]
        public async Task<HttpResponseMessage> Update()
        {
            var nodeConnectionInfo = await ReadJsonObjectAsync<NodeConnectionInfo>().ConfigureAwait(false);
            if (nodeConnectionInfo == null)
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            await ClusterManager.Client.SendNodeUpdateAsync(nodeConnectionInfo).ConfigureAwait(false);

            return GetEmptyMessage();
        }

        [HttpGet]
        [RavenRoute("admin/cluster/canJoin")]
        public Task<HttpResponseMessage> CanJoin([FromUri] Guid topologyId)
        {
            var topology = ClusterManager.Engine.CurrentTopology;
            if (topology.TopologyId == topologyId)
                return GetEmptyMessageAsTask(HttpStatusCode.NotModified);

            if (topology.AllNodes.Any())
                return GetMessageWithStringAsTask("Can't join node to cluster. Node is in different cluster", HttpStatusCode.NotAcceptable);

            var nextStart = 0;
            var hasAnyDatabase = SystemDatabase.Documents
                .GetDocumentsWithIdStartingWith(Constants.Database.Prefix, null, null, 0, 1, SystemDatabase.WorkContext.CancellationToken, ref nextStart)
                .Length > 0;

            if (hasAnyDatabase)
            {
                return GetMessageWithStringAsTask("Can't join node to cluster. Node is not empty", HttpStatusCode.Conflict);
            }
            //if i'm hot spare i should be joined as a non-voter
            if (RequestManager.IsInHotSpareMode)
                return GetMessageWithStringAsTask("NonVoter", HttpStatusCode.Accepted);

            return GetMessageWithStringAsTask("Voter", HttpStatusCode.Accepted);
        }

        [HttpGet]
        [RavenRoute("admin/cluster/leave")]
        public async Task<HttpResponseMessage> Leave([FromUri] Guid name)
        {
            var nodeName = RaftHelper.GetNodeName(name);

            if (ClusterManager.Engine.CurrentTopology.Contains(nodeName) == false)
                return GetEmptyMessage(HttpStatusCode.NotModified);

            var node = ClusterManager.Engine.CurrentTopology.GetNodeByName(nodeName);
            await ClusterManager.Client.SendLeaveAsync(node).ConfigureAwait(false);

            return GetMessageWithObject(new
            {
                Removed = name
            });
        }

        [HttpPost]
        [RavenRoute("admin/cluster/changeVotingMode")]
        public async Task<HttpResponseMessage> ChangeVotingMode([FromUri] bool isVoting)
        {
            var nodeConnectionInfo = await ReadJsonObjectAsync<NodeConnectionInfo>().ConfigureAwait(false);
            //if i'm the leader and i was just requested to stop been a voter than i should step down
            //this is a diffrent behavior than update because this is needed for HotSpare.
            if (nodeConnectionInfo == ClusterManager.Engine.Options.SelfConnection && !isVoting)
                await ClusterManager.Engine.StepDownAsync().ConfigureAwait(false);
            if (ClusterManager.Engine.State == RaftEngineState.Leader)
            {
                await ClusterManager.Engine.ModifyNodeVotingModeAsync(nodeConnectionInfo, isVoting).ConfigureAwait(false);
            }
            else
            {
                await ClusterManager.Client.SendVotingModeChangeRequestAsync(nodeConnectionInfo, isVoting).ConfigureAwait(false);
            }            
            return GetEmptyMessage();
        }
    }
}
