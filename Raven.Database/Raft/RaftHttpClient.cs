// -----------------------------------------------------------------------
//  <copyright file="RaftHttpClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using Rachis;
using Rachis.Transport;
using Rachis.Utils;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Raft.Commands;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Json.Linq;

namespace Raven.Database.Raft
{
	public class RaftHttpClient
	{
		private readonly RaftEngine raftEngine;

		private NodeConnectionInfo SelfConnection
		{
			get
			{
				return raftEngine.Options.SelfConnection;
			}
		}

		private readonly HttpClient httpClient;

		public RaftHttpClient(RaftEngine raftEngine)
		{
			this.raftEngine = raftEngine;
			httpClient = new HttpClient();
		}

		public async Task SendJoinServerAsync(NodeConnectionInfo nodeConnectionInfo)
		{
			try
			{
				await raftEngine.AddToClusterAsync(nodeConnectionInfo);
				return;
			}
			catch (NotLeadingException)
			{
			}
			await JoinAsync(raftEngine.GetLeaderNode(), nodeConnectionInfo);
		}

		public Task SendClusterConfigurationAsync(ClusterConfiguration configuration)
		{
			try
			{
				var command = ClusterConfigurationUpdateCommand.Create(configuration);
				raftEngine.AppendCommand(command);
				return command.Completion.Task;
			}
			catch (NotLeadingException)
			{
				var node = raftEngine.GetLeaderNode();
				var url = node.Uri.AbsoluteUri + "admin/raft/commands/cluster/configuration";
				return SendClusterConfigurationInternalAsync(url, configuration);
			}
		}

		public async Task SendClusterConfigurationInternalAsync(string url, ClusterConfiguration configuration)
		{
			var response = await httpClient.PutAsync(url, new JsonContent(RavenJObject.FromObject(configuration))).ConfigureAwait(false);
			if (response.IsSuccessStatusCode) 
				return;

			switch (response.StatusCode)
			{
				case HttpStatusCode.Redirect:
					await SendClusterConfigurationInternalAsync(response.Headers.Location.AbsoluteUri, configuration).ConfigureAwait(false);
					return;
				default:
					throw new NotImplementedException(response.StatusCode.ToString());	// TODO [ppekrol]
			}
		}

		public async Task<CanJoinResult> CanJoinAsync(string destinationNodeUrl)
		{
			var url = destinationNodeUrl + "admin/raft/canJoin?name=" + SelfConnection.Name;

			var response = await ExecuteWithRetriesAsync(() => httpClient.GetAsync(url).ConfigureAwait(false));

			if (response.IsSuccessStatusCode)
				return CanJoinResult.CanJoin;

			switch (response.StatusCode)
			{
				case HttpStatusCode.NotModified:
					return CanJoinResult.AlreadyJoined;
				case HttpStatusCode.NotAcceptable:
					return CanJoinResult.InAnotherCluster;
				default:
					throw new NotImplementedException(response.StatusCode.ToString());	// TODO [ppekrol]
			}
		}

		public async Task<CanJoinResult> JoinAsync(NodeConnectionInfo leaderNode, NodeConnectionInfo newNode)
		{
			var url = leaderNode.Uri + "admin/raft/join";
			var content = new JsonContent(RavenJToken.FromObject(newNode));

			var response = await ExecuteWithRetriesAsync(() => httpClient.PostAsync(url, content).ConfigureAwait(false));

			if (response.IsSuccessStatusCode)
				return CanJoinResult.CanJoin;

			switch (response.StatusCode)
			{
				case HttpStatusCode.NotModified:
					return CanJoinResult.AlreadyJoined;
				case HttpStatusCode.NotAcceptable:
					return CanJoinResult.InAnotherCluster;
				default:
					throw new NotImplementedException(response.StatusCode.ToString());	// TODO [ppekrol]
			}
		}

		public async Task LeaveAsync()
		{
			if (raftEngine.IsLeader())
			{
				await raftEngine.StepDownAsync().ConfigureAwait(false);
				raftEngine.WaitForLeader();
			}

			var node = raftEngine.GetLeaderNode() ?? raftEngine.GetFirstNonSelfNode();
			var url = node.Uri + "admin/raft/leave?name=" + SelfConnection.Name;

			var response = await httpClient.GetAsync(url).ConfigureAwait(false);
			if (response.IsSuccessStatusCode)
				return;

			switch (response.StatusCode)
			{
				case HttpStatusCode.NotModified:
					return; // not in topology
				case HttpStatusCode.Redirect:
					throw new NotImplementedException(); // not a leader, redirect		// TODO [ppekrol]
				default:
					throw new NotImplementedException(response.StatusCode.ToString());	// TODO [ppekrol]
			}
		}

		private static async Task<HttpResponseMessage> ExecuteWithRetriesAsync(Func<ConfiguredTaskAwaitable<HttpResponseMessage>> action, int numberOfRetries = 3)
		{
			if (numberOfRetries <= 0)
				throw new InvalidOperationException("Number of tries must be greater than 0.");

			var numberOfErrors = 0;
			while (true)
			{
				var response = await action();
				if (response.IsSuccessStatusCode)
					return response;

				if (response.StatusCode != HttpStatusCode.ServiceUnavailable)
					return response;

				numberOfErrors++;

				if (numberOfErrors >= numberOfRetries)
					throw new InvalidOperationException("Could not connect to node.");
			}
		}

		public async Task<Guid> GetDatabaseId(NodeConnectionInfo nodeConnectionInfo)
		{
			var response = await httpClient.GetAsync(nodeConnectionInfo.Uri + "/stats").ConfigureAwait(false);
			if (!response.IsSuccessStatusCode)
				throw new InvalidOperationException("Unable to fetch database statictics for: " + nodeConnectionInfo.Uri);

			using (var responseStream = await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
			{
				var json = RavenJToken.TryLoad(responseStream);
				var stats = json.JsonDeserialization<DatabaseStatistics>();
				return stats.DatabaseId;
			}
		}
	}

	public enum CanJoinResult
	{
		CanJoin,

		AlreadyJoined,

		InAnotherCluster
	}
}