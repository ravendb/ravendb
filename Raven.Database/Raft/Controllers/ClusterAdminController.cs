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

using Rachis.Transport;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util;

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

            await ClusterManager.Client.SendClusterConfigurationAsync(configuration).ConfigureAwait(false);
            return GetEmptyMessage();
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

            return GetEmptyMessage(HttpStatusCode.Created);
        }

        [HttpPatch]
        [RavenRoute("admin/cluster/initialize-new-cluster/{*id}")]
        public HttpResponseMessage InitializeNewCluster(string id)
        {
            if (string.IsNullOrEmpty(id))
                ClusterManager.InitializeTopology(isPartOfExistingCluster: true);
            else
                ClusterManager.InitializeEmptyTopologyWithId(Guid.Parse(id));

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

            bool forced;
            bool.TryParse(GetQueryStringValue("force"), out forced);

            var topology = ClusterManager.Engine.CurrentTopology;

            if (forced == false)
            {
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
            }
            else
            {
                await ClusterManager.Client.SendInitializeNewClusterForAsync(nodeConnectionInfo, topology.TopologyId).ConfigureAwait(false);
            }
            
            if (topology.Contains(nodeConnectionInfo.Name))
                return GetEmptyMessage(HttpStatusCode.NotModified);
            
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

            return GetEmptyMessageAsTask(HttpStatusCode.Accepted);
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
    }
}
