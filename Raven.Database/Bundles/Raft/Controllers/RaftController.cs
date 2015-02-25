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

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Database.Bundles.Raft.Util;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Raft.Controllers
{
	public class RaftController : RavenDbApiController
	{
		private RaftEngine RaftEngine
		{
			get
			{
				return Database.RaftEngine;
			}
		}

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
		[RavenRoute("databases/{databaseName}/raft/topology")]
		public HttpResponseMessage Topology()
		{
			return Request.CreateResponse(HttpStatusCode.OK, new
			{
				Database.RaftEngine.CurrentLeader,
				Database.RaftEngine.PersistentState.CurrentTerm,
				State = Database.RaftEngine.State.ToString(),
				Database.RaftEngine.CommitIndex,
				Database.RaftEngine.CurrentTopology.AllVotingNodes,
				Database.RaftEngine.CurrentTopology.PromotableNodes,
				Database.RaftEngine.CurrentTopology.NonVotingNodes,
				Database.RaftEngine.CurrentTopology.TopologyId
			});
		}

		[HttpGet]
		[RavenRoute("raft/join")]
		[RavenRoute("databases/{databaseName}/raft/join")]
		public async Task<HttpResponseMessage> Join([FromUri] string url, [FromUri] string name)
		{
			var nodeUri = new Uri(RaftHelper.NormalizeNodeUrl(url));
			var nodeName = Guid.Parse(name);

			if (RaftEngine.State != RaftEngineState.Leader)
				return HandleNonLeader();

			if (RaftEngine.CurrentTopology.Contains(name))
				return GetEmptyMessage(HttpStatusCode.NotModified);

			var document = Database
				.ConfigurationRetriever
				.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);

			if (document == null || document.MergedDocument.Destinations.Count == 0 || await IsSourceValidAsync(document.MergedDocument, nodeName) == false)
				return GetEmptyMessage(HttpStatusCode.NotAcceptable);

			await RaftEngine.AddToClusterAsync(new NodeConnectionInfo
			{
				Name = name,
				Uri = nodeUri
			});

			return GetEmptyMessage(HttpStatusCode.Accepted);
		}

		[HttpGet]
		[RavenRoute("raft/join")]
		[RavenRoute("databases/{databaseName}/raft/join/me")]
		public async Task<HttpResponseMessage> JoinMe([FromUri] string url, [FromUri] string name)
		{
			var nodeName = Guid.Parse(name);

			if (RaftEngine.CurrentTopology.Contains(name))
				return GetEmptyMessage(HttpStatusCode.NotModified);

			var document = Database
				.ConfigurationRetriever
				.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);

			if (document == null || document.MergedDocument.Destinations.Count == 0 || await IsSourceValidAsync(document.MergedDocument, nodeName) == false)
				return GetEmptyMessage(HttpStatusCode.NotAcceptable);

			if (RaftEngine.IsLeader() || RaftEngine.CurrentTopology.QuorumSize <= 1)
			{
				await new RaftHttpClient(RaftEngine).JoinAsync(url);
				return GetEmptyMessage(HttpStatusCode.Accepted);
			}

			throw new NotImplementedException();
		}

		private static async Task<bool> IsSourceValidAsync(ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> document, Guid nodeName)
		{
			// TODO [ppekrol] optimize this

			using (var httpClient = new HttpClient())
			{
				foreach (var destination in document.Destinations)
				{
					var url = destination.Url.ForDatabase(destination.Database) + "/stats";
					var response = await httpClient.GetAsync(url);
					if (response.IsSuccessStatusCode == false)
						continue;

					using (var stream = await response.GetResponseStreamWithHttpDecompression())
					{
						var data = (RavenJObject)RavenJToken.TryLoad(stream);
						var stats = data.JsonDeserialization<DatabaseStatistics>();

						if (stats.DatabaseId == nodeName)
							return true;
					}
				}
			}

			return false;
		}

		[HttpGet]
		[RavenRoute("raft/leave")]
		[RavenRoute("databases/{databaseName}/raft/leave")]
		public async Task<HttpResponseMessage> Leave([FromUri] string name)
		{
			if (RaftEngine.State != RaftEngineState.Leader)
				return HandleNonLeader();

			if (RaftEngine.CurrentTopology.Contains(name) == false)
				return GetEmptyMessage(HttpStatusCode.NotModified);

			await Database.RaftEngine.RemoveFromClusterAsync(new NodeConnectionInfo
			{
				Name = name
			});

			return new HttpResponseMessage(HttpStatusCode.Accepted);
		}

		[HttpPost]
		[RavenRoute("raft/installSnapshot")]
		[RavenRoute("databases/{databaseName}/raft/installSnapshot")]
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
		[RavenRoute("databases/{databaseName}/raft/appendEntries")]
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
		[RavenRoute("databases/{databaseName}/raft/requestVote")]
		public Task<HttpResponseMessage> RequestVote([FromUri]RequestVoteRequest request)
		{
			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
			Bus.Publish(request, taskCompletionSource);
			return taskCompletionSource.Task;
		}

		[HttpGet]
		[RavenRoute("raft/timeoutNow")]
		[RavenRoute("databases/{databaseName}/raft/timeoutNow")]
		public Task<HttpResponseMessage> TimeoutNow([FromUri]TimeoutNowRequest request)
		{
			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
			Bus.Publish(request, taskCompletionSource);
			return taskCompletionSource.Task;
		}

		[HttpGet]
		[RavenRoute("raft/disconnectFromCluster")]
		[RavenRoute("databases/{databaseName}/raft/disconnectFromCluster")]
		public Task<HttpResponseMessage> DisconnectFromCluster([FromUri]DisconnectedFromCluster request)
		{
			var taskCompletionSource = new TaskCompletionSource<HttpResponseMessage>();
			Bus.Publish(request, taskCompletionSource);
			return taskCompletionSource.Task;
		}

		[HttpGet]
		[RavenRoute("raft/canInstallSnapshot")]
		[RavenRoute("databases/{databaseName}/raft/canInstallSnapshot")]
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

		private HttpResponseMessage HandleNonLeader()
		{
			var leaderNode = RaftEngine.GetLeaderNode();

			var message = new HttpResponseMessage(HttpStatusCode.Redirect);
			message.Headers.Location = leaderNode.Uri;
			return message;
		}
	}
}