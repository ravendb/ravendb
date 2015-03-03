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

using Rachis;
using Rachis.Messages;
using Rachis.Storage;
using Rachis.Transport;

using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Raft.Controllers
{
	public class RaftController : BaseAdminController
	{
		private HttpTransport Transport
		{
			get
			{
				return (HttpTransport)RaftEngine.Transport;
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
				RaftEngine.CurrentLeader,
				RaftEngine.PersistentState.CurrentTerm,
				State = RaftEngine.State.ToString(),
				RaftEngine.CommitIndex,
				RaftEngine.CurrentTopology.AllVotingNodes,
				RaftEngine.CurrentTopology.PromotableNodes,
				RaftEngine.CurrentTopology.NonVotingNodes,
				RaftEngine.CurrentTopology.TopologyId
			});
		}

		[HttpPut]
		[RavenRoute("admin/raft/commands/cluster/configuration")]
		public async Task<HttpResponseMessage> ClusterConfiguration()
		{
			var configuration = await ReadJsonObjectAsync<ClusterConfiguration>();
			if (configuration == null) 
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			var client = new RaftHttpClient(RaftEngine);
			await client.SendClusterConfigurationAsync(configuration).ConfigureAwait(false);
			return GetEmptyMessage();
		}

		[HttpPost]
		[RavenRoute("admin/raft/create")]
		public async Task<HttpResponseMessage> Create()
		{
			var topology = RaftEngine.CurrentTopology;

			if (RaftEngine.IsLeader())
				return await GetEmptyMessageAsTask(HttpStatusCode.NotModified);

			if (topology.AllNodes.Any())
				return await GetEmptyMessageAsTask(HttpStatusCode.NotAcceptable);

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

			var client = new RaftHttpClient(RaftEngine);

			if (nodeConnectionInfo.Name == null)
				nodeConnectionInfo.Name = RaftHelper.GetNodeName(await client.GetDatabaseId(nodeConnectionInfo));

			if (await client.SendCanJoinAsync(nodeConnectionInfo).ConfigureAwait(false) != CanJoinResult.CanJoin)
				return await GetEmptyMessageAsTask(HttpStatusCode.BadRequest);

			var topology = RaftEngine.CurrentTopology;
			if (topology.Contains(nodeConnectionInfo.Name))
				return await GetEmptyMessageAsTask(HttpStatusCode.NotModified);

			await client.SendJoinServerAsync(nodeConnectionInfo).ConfigureAwait(false);
			return GetEmptyMessage();
		}

		
		[HttpGet]
		[RavenRoute("admin/raft/canJoin")]
		public Task<HttpResponseMessage> CanJoin([FromUri] string name)
		{
			var topology = RaftEngine.CurrentTopology;
			if (topology.Contains(name))
				return GetEmptyMessageAsTask(HttpStatusCode.NotModified);

			if (topology.AllNodes.Any())
				return GetEmptyMessageAsTask(HttpStatusCode.NotAcceptable);

			return GetEmptyMessageAsTask(HttpStatusCode.Accepted);
		}

		[HttpGet]
		[RavenRoute("admin/raft/leave")]
		public async Task<HttpResponseMessage> Leave([FromUri] Guid name)
		{
			var nodeName = RaftHelper.GetNodeName(name);

			if (RaftEngine.CurrentTopology.Contains(nodeName) == false)
				return GetEmptyMessage(HttpStatusCode.NotModified);

			var node = RaftEngine.CurrentTopology.GetNodeByName(nodeName);
			var client = new RaftHttpClient(RaftEngine);
			await client.SendLeaveAsync(node);

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
			var leaderNode = RaftEngine.GetLeaderNode();

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
				Query =  Request.RequestUri.Query.TrimStart('?'),
				Fragment = Request.RequestUri.Fragment
			}.Uri;

			return message;
		}
	}
}