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

		public async Task<CanJoinResult> CanJoinAsync(string destinationNodeUrl, int numberOfTries = 3)
		{
			var url = destinationNodeUrl + "admin/raft/canJoin?name=" + SelfConnection.Name;

			var response = await httpClient.GetAsync(url).ConfigureAwait(false);
			if (response.IsSuccessStatusCode)
				return CanJoinResult.CanJoin;

			numberOfTries--;

			switch (response.StatusCode)
			{
				case HttpStatusCode.NotModified:
					return CanJoinResult.AlreadyJoined;
				case HttpStatusCode.NotAcceptable:
					return CanJoinResult.InAnotherCluster;
				case HttpStatusCode.ServiceUnavailable:
					if (numberOfTries > 0)
						return await CanJoinAsync(destinationNodeUrl, numberOfTries);
					throw new InvalidOperationException(string.Format("Could not connect to '{0}'.", destinationNodeUrl));
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
	}

	public enum CanJoinResult
	{
		CanJoin,

		AlreadyJoined,

		InAnotherCluster
	}
}