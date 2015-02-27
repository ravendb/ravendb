// -----------------------------------------------------------------------
//  <copyright file="RaftHttpClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

using Rachis;
using Rachis.Transport;

using Raven.Database.Raft.Util;

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

		public async Task<CanJoinResult> CanJoinAsync(string destinationNodeUrl)
		{
			var url = destinationNodeUrl + "admin/raft/canJoin?name=" + SelfConnection.Name;

			var response = await ExecuteWithRetriesAsync(() => httpClient.GetAsync(url));
			
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

		public async Task<CanJoinResult> JoinAsync(string leaderNodeUrl, string nodeName)
		{
			var url = leaderNodeUrl + "admin/raft/join?name=" + nodeName;

			var response = await ExecuteWithRetriesAsync(() => httpClient.GetAsync(url));

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
			var url = node.Uri + "admin//raft/leave?name=" + SelfConnection.Name;

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

		private static async Task<HttpResponseMessage> ExecuteWithRetriesAsync(Func<Task<HttpResponseMessage>> action, int numberOfRetries = 3)
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
	}

	public enum CanJoinResult
	{
		CanJoin,

		AlreadyJoined,

		InAnotherCluster
	}
}