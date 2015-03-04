// -----------------------------------------------------------------------
//  <copyright file="RaftController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

using Newtonsoft.Json;

using Rachis.Messages;
using Rachis.Storage;
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
	public class RaftController : BaseAdminController
	{
		private HttpTransport Transport
		{
			get
			{
				return (HttpTransport)RaftEngine.Engine.Transport;
			}
		}

		private HttpTransportBus Bus
		{
			get
			{
				return Transport.Bus;
			}
		}

		[HttpGet]
		[RavenRoute("raft/topology")]
		public HttpResponseMessage Topology()
		{
			return Request.CreateResponse(HttpStatusCode.OK, new
			{
				RaftEngine.Engine.CurrentLeader,
				RaftEngine.Engine.PersistentState.CurrentTerm,
				State = RaftEngine.Engine.State.ToString(),
				RaftEngine.Engine.CommitIndex,
				RaftEngine.Engine.CurrentTopology.AllVotingNodes,
				RaftEngine.Engine.CurrentTopology.PromotableNodes,
				RaftEngine.Engine.CurrentTopology.NonVotingNodes,
				RaftEngine.Engine.CurrentTopology.TopologyId
			});
		}

		[HttpPut]
		[RavenRoute("admin/raft/commands/cluster/configuration")]
		public async Task<HttpResponseMessage> ClusterConfiguration()
		{
			var configuration = await ReadJsonObjectAsync<ClusterConfiguration>();
			if (configuration == null)
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			await RaftEngine.Client.SendClusterConfigurationAsync(configuration).ConfigureAwait(false);
			return GetEmptyMessage();
		}

		[HttpPut]
		[RavenRoute("admin/raft/commands/cluster/database/{*id}")]
		public async Task<HttpResponseMessage> CreateDatabase(string id)
		{
			var document = await ReadJsonObjectAsync<DatabaseDocument>();
			if (document == null)
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			if (document.IsClusterDatabase() == false)
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			await RaftEngine.Client.SendDatabaseUpdateAsync(id, document).ConfigureAwait(false);
			return GetEmptyMessage();
		}

		[HttpDelete]
		[RavenRoute("admin/raft/commands/cluster/database/{*id}")]
		public async Task<HttpResponseMessage> DeleteDatabase(string id)
		{
			bool result;
			var hardDelete = bool.TryParse(GetQueryStringValue("hard-delete"), out result) && result;

			if (string.IsNullOrEmpty(id))
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			var documentJson = Database.Documents.Get(DatabaseHelper.GetDatabaseKey(id), null);
			if (documentJson == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			var document = documentJson.DataAsJson.JsonDeserialization<DatabaseDocument>();
			if (document.IsClusterDatabase() == false)
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			await RaftEngine.Client.SendDatabaseDeleteAsync(id, hardDelete).ConfigureAwait(false);
			return GetEmptyMessage();
		}

		[HttpPost]
		[RavenRoute("admin/raft/create")]
		public async Task<HttpResponseMessage> Create()
		{
			var topology = RaftEngine.Engine.CurrentTopology;

			if (RaftEngine.IsLeader())
				return await GetEmptyMessageAsTask(HttpStatusCode.NotModified);

			if (topology.AllNodes.Any())
				return GetMessageWithString("Server is already in cluster.", HttpStatusCode.NotAcceptable);

			var nodeConnectionInfo = await ReadJsonObjectAsync<NodeConnectionInfo>();

			RaftEngineFactory.InitializeTopology(nodeConnectionInfo, RaftEngine);

			return await GetEmptyMessageAsTask(HttpStatusCode.Created);
		}

		[HttpPost]
		[RavenRoute("admin/raft/join")]
		public async Task<HttpResponseMessage> JoinToCluster()
		{
			var nodeConnectionInfo = await ReadJsonObjectAsync<NodeConnectionInfo>();
			if (nodeConnectionInfo == null)
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			if (nodeConnectionInfo.Name == null)
				nodeConnectionInfo.Name = RaftHelper.GetNodeName(await RaftEngine.Client.GetDatabaseId(nodeConnectionInfo));

			var canJoinResult = await RaftEngine.Client.SendCanJoinAsync(nodeConnectionInfo).ConfigureAwait(false);
			switch (canJoinResult)
			{
				case CanJoinResult.InAnotherCluster:
					return GetMessageWithString("Can't join node to cluster. Node is in different cluster", HttpStatusCode.BadRequest);
				case CanJoinResult.AlreadyJoined:
					return GetEmptyMessage(HttpStatusCode.NotModified);
			}

			var topology = RaftEngine.Engine.CurrentTopology;
			if (topology.Contains(nodeConnectionInfo.Name))
				return GetEmptyMessage(HttpStatusCode.NotModified);

			await RaftEngine.Client.SendJoinServerAsync(nodeConnectionInfo).ConfigureAwait(false);
			return GetEmptyMessage();
		}


		[HttpGet]
		[RavenRoute("admin/raft/canJoin")]
		public Task<HttpResponseMessage> CanJoin([FromUri] string name)
		{
			var topology = RaftEngine.Engine.CurrentTopology;
			if (topology.Contains(name))
				return GetEmptyMessageAsTask(HttpStatusCode.NotModified);

			if (topology.AllNodes.Any())
				return GetMessageWithStringAsTask("Can't join node to cluster. Node is in different cluster", HttpStatusCode.NotAcceptable);

			return GetEmptyMessageAsTask(HttpStatusCode.Accepted);
		}

		[HttpGet]
		[RavenRoute("admin/raft/leave")]
		public async Task<HttpResponseMessage> Leave([FromUri] Guid name)
		{
			var nodeName = RaftHelper.GetNodeName(name);

			if (RaftEngine.Engine.CurrentTopology.Contains(nodeName) == false)
				return GetEmptyMessage(HttpStatusCode.NotModified);

			var node = RaftEngine.Engine.CurrentTopology.GetNodeByName(nodeName);
			await RaftEngine.Client.SendLeaveAsync(node);

			return GetMessageWithObject(new
			{
				Removed = name
			});
		}

		[HttpPost]
		[RavenRoute("raft/installSnapshot")]
		public async Task<HttpResponseMessage> InstallSnapshot([FromUri]InstallSnapshotRequest request, [FromUri]string topology)
		{
			request.Topology = JsonConvert.DeserializeObject<Topology>(topology);
			var stream = await Request.Content.ReadAsStreamAsync();
			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
			Bus.Publish(request, taskCompletionSource, stream);
			return await taskCompletionSource.Task;
		}

		[HttpPost]
		[RavenRoute("raft/appendEntries")]
		public async Task<HttpResponseMessage> AppendEntries([FromUri]AppendEntriesRequest request, [FromUri]int entriesCount)
		{
			var stream = await Request.Content.ReadAsStreamAsync();
			request.Entries = new LogEntry[entriesCount];
			for (int i = 0; i < entriesCount; i++)
			{
				var index = Read7BitEncodedInt(stream);
				var term = Read7BitEncodedInt(stream);
				var isTopologyChange = stream.ReadByte() == 1;
				var lengthOfData = (int)Read7BitEncodedInt(stream);
				request.Entries[i] = new LogEntry
				{
					Index = index,
					Term = term,
					IsTopologyChange = isTopologyChange,
					Data = new byte[lengthOfData]
				};

				var start = 0;
				while (start < lengthOfData)
				{
					var read = stream.Read(request.Entries[i].Data, start, lengthOfData - start);
					start += read;
				}
			}

			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
			Bus.Publish(request, taskCompletionSource);
			return await taskCompletionSource.Task;
		}

		[HttpGet]
		[RavenRoute("raft/requestVote")]
		public Task<HttpResponseMessage> RequestVote([FromUri]RequestVoteRequest request)
		{
			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
			Bus.Publish(request, taskCompletionSource);
			return taskCompletionSource.Task;
		}

		[HttpGet]
		[RavenRoute("raft/timeoutNow")]
		public Task<HttpResponseMessage> TimeoutNow([FromUri]TimeoutNowRequest request)
		{
			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
			Bus.Publish(request, taskCompletionSource);
			return taskCompletionSource.Task;
		}

		[HttpGet]
		[RavenRoute("raft/disconnectFromCluster")]
		public Task<HttpResponseMessage> DisconnectFromCluster([FromUri]DisconnectedFromCluster request)
		{
			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
			Bus.Publish(request, taskCompletionSource);
			return taskCompletionSource.Task;
		}

		[HttpGet]
		[RavenRoute("raft/canInstallSnapshot")]
		public Task<HttpResponseMessage> CanInstallSnapshot([FromUri]CanInstallSnapshotRequest request)
		{
			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
			Bus.Publish(request, taskCompletionSource);
			return taskCompletionSource.Task;
		}

		private static long Read7BitEncodedInt(Stream stream)
		{
			long count = 0;
			int shift = 0;
			byte b;
			do
			{
				if (shift == 9 * 7)
					throw new InvalidDataException("Invalid 7bit shifted value, used more than 9 bytes");

				var maybeEof = stream.ReadByte();
				if (maybeEof == -1)
					throw new EndOfStreamException();

				b = (byte)maybeEof;
				count |= (uint)(b & 0x7F) << shift;
				shift += 7;
			} while ((b & 0x80) != 0);
			return count;
		}

		private HttpResponseMessage RedirectToLeader()
		{
			var leaderNode = RaftEngine.Engine.GetLeaderNode();

			if (leaderNode == null)
			{
				return Request.CreateResponse(HttpStatusCode.BadRequest, new
				{
					Error = "There is no current leader, try again later"
				});
			}

			var message = Request.CreateResponse(HttpStatusCode.Redirect);
			message.Headers.Location = new UriBuilder(leaderNode.Uri)
			{
				Path = Request.RequestUri.LocalPath,
				Query = Request.RequestUri.Query.TrimStart('?'),
				Fragment = Request.RequestUri.Fragment
			}.Uri;

			return message;
		}
	}
}