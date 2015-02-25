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

using Raven.Database.Bundles.Raft.Util;

namespace Raven.Database.Bundles.Raft
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

		public async Task JoinAsync(string destinationNodeUrl, int numberOfTries = 3)
		{
			var url = destinationNodeUrl + "/raft/join?url=" + SelfConnection.Uri + "&name=" + SelfConnection.Name;

			var response = await httpClient.GetAsync(url).ConfigureAwait(false);
			if (response.IsSuccessStatusCode)
				return;

			numberOfTries--;

			switch (response.StatusCode)
			{
				case HttpStatusCode.NotModified:
					return; // already in topology
				case HttpStatusCode.NotAcceptable:
					return; // not in destination document => no master/master
				case HttpStatusCode.Redirect:
					throw new NotImplementedException(); // not a leader, redirect		// TODO [ppekrol]
				case HttpStatusCode.ServiceUnavailable:
					if (numberOfTries > 0)
						await JoinAsync(destinationNodeUrl, numberOfTries);
					return;
				default:
					throw new NotImplementedException(response.StatusCode.ToString());	// TODO [ppekrol]
			}
		}

		public async Task JoinMeAsync(string destinationNodeUrl, int numberOfTries = 2)
		{
			var url = destinationNodeUrl + "/raft/join/me?url=" + SelfConnection.Uri + "&name=" + SelfConnection.Name;

			var response = await httpClient.GetAsync(url).ConfigureAwait(false);
			if (response.IsSuccessStatusCode)
				return;

			numberOfTries--;

			switch (response.StatusCode)
			{
				case HttpStatusCode.NotModified:
					return; // already in topology
				case HttpStatusCode.NotAcceptable:
					return; // not in destination document => no master/master
				case HttpStatusCode.ServiceUnavailable:
					if (numberOfTries > 0)
						await JoinAsync(destinationNodeUrl, numberOfTries);
					return;
				default:
					throw new NotImplementedException(response.StatusCode.ToString());	// TODO [ppekrol]
			}
		}

		public async Task LeaveAsync()
		{
			if (raftEngine.IsLeader())
				await raftEngine.StepDownAsync().ConfigureAwait(false);

			var node = raftEngine.GetLeaderNode() ?? raftEngine.GetFirstNonSelfNode();
			var url = node.Uri + "/raft/leave?name=" + SelfConnection.Name;

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
}