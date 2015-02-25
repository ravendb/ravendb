// -----------------------------------------------------------------------
//  <copyright file="RaftHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Rachis;
using Rachis.Transport;

namespace Raven.Database.Bundles.Raft.Util
{
	public static class RaftHelper
	{
		public static NodeConnectionInfo GetLeaderNode(this RaftEngine engine)
		{
			var leader = engine.CurrentLeader;
			if (leader == null)
				return null;

			return engine.CurrentTopology.AllNodes.Single(x => x.Name == leader);
		}

		public static bool IsLeader(this RaftEngine engine)
		{
			return engine.State == RaftEngineState.Leader;
		}

		public static NodeConnectionInfo GetFirstNonSelfNode(this RaftEngine engine)
		{
			var selfNode = engine.Options.SelfConnection;

			return engine.CurrentTopology.AllNodes.First(x => x.Name != selfNode.Name);
		}

		public static string NormalizeNodeUrl(string url)
		{
			if (string.IsNullOrEmpty(url))
				throw new ArgumentNullException("url");

			if (url.EndsWith("/") == false && url.EndsWith("\\") == false)
				url += "/";

			return url;
		}
	}
}