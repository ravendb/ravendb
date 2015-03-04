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
			await SendJoinServerInternalAsync(raftEngine.GetLeaderNode(), nodeConnectionInfo);
		}

		public async Task<CanJoinResult> SendJoinServerInternalAsync(NodeConnectionInfo leaderNode, NodeConnectionInfo newNode)
		{
			var url = leaderNode.Uri.AbsoluteUri + "admin/raft/join";
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
				return SendClusterConfigurationInternalAsync(raftEngine.GetLeaderNode(), configuration);
			}
		}

		public Task SendDatabaseUpdateAsync(string databaseName, DatabaseDocument document)
		{
			try
			{
				var command = DatabaseUpdateCommand.Create(databaseName, document);
				raftEngine.AppendCommand(command);
				return command.Completion.Task;
			}
			catch (NotLeadingException)
			{
				return SendDatabaseUpdateInternalAsync(raftEngine.GetLeaderNode(), databaseName, document);
			}
		}

		public Task SendDatabaseDeleteAsync(string databaseName, bool hardDelete)
		{
			try
			{
				var command = DatabaseDeletedCommand.Create(databaseName, hardDelete);
				raftEngine.AppendCommand(command);
				return command.Completion.Task;
			}
			catch (NotLeadingException)
			{
				return SendDatabaseDeleteInternalAsync(raftEngine.GetLeaderNode(), databaseName, hardDelete);
			}
		}

		private async Task SendDatabaseDeleteInternalAsync(NodeConnectionInfo node, string databaseName, bool hardDelete)
		{
			var url = node.Uri.AbsoluteUri + "admin/raft/commands/cluster/database/" + Uri.EscapeDataString(databaseName) + "?hardDelete=" + hardDelete;
			var response = await httpClient.DeleteAsync(url).ConfigureAwait(false);
			if (response.IsSuccessStatusCode)
				return;

			throw new NotImplementedException(response.StatusCode.ToString());	// TODO [ppekrol]
		}

		private Task SendClusterConfigurationInternalAsync(NodeConnectionInfo leaderNode, ClusterConfiguration configuration)
		{
			return PutAsync(leaderNode, "admin/raft/commands/cluster/configuration", configuration);
		}

		private Task SendDatabaseUpdateInternalAsync(NodeConnectionInfo leaderNode, string databaseName, DatabaseDocument document)
		{
			return PutAsync(leaderNode, "admin/raft/commands/cluster/database/" + Uri.EscapeDataString(databaseName), document);
		}

		private async Task PutAsync(NodeConnectionInfo node, string action, object content)
		{
			var url = node.Uri.AbsoluteUri + action;
			var response = await httpClient.PutAsync(url, new JsonContent(RavenJObject.FromObject(content))).ConfigureAwait(false);
			if (response.IsSuccessStatusCode)
				return;

			throw new NotImplementedException(response.StatusCode.ToString());	// TODO [ppekrol]
		}

		public async Task<CanJoinResult> SendCanJoinAsync(NodeConnectionInfo nodeConnectionInfo)
		{
			var url = nodeConnectionInfo.Uri.AbsoluteUri + "admin/raft/canJoin?name=" + SelfConnection.Name;

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

		public async Task SendLeaveAsync(NodeConnectionInfo node)
		{
			try
			{
				if (raftEngine.GetLeaderNode() == node)
				{
					await raftEngine.StepDownAsync().ConfigureAwait(false);
					raftEngine.WaitForLeader();
				}
				else
				{
					await raftEngine.RemoveFromClusterAsync(node);
				}
			}
			catch (NotLeadingException e)
			{
			}
			await SendLeaveClusterInternalAsync(raftEngine.GetLeaderNode(), node);
		}

		public async Task SendLeaveClusterInternalAsync(NodeConnectionInfo leaderNode, NodeConnectionInfo leavingNode)
		{
			var url = leavingNode.Uri.AbsoluteUri + "admin/raft/leave?name=" + leavingNode.Name;
			var response = await httpClient.GetAsync(url).ConfigureAwait(false);
			if (response.IsSuccessStatusCode)
				return;

			throw new NotImplementedException(response.StatusCode.ToString());	// TODO [ppekrol]
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