// -----------------------------------------------------------------------
//  <copyright file="RaftHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;

using Rachis;
using Rachis.Transport;

using Raven.Abstractions.Data;

namespace Raven.Database.Raft.Util
{
	public static class RaftHelper
	{
		public static bool IsActive(this RavenRaftEngine engine)
		{
			return engine.Engine.CurrentTopology.AllNodes.Any();
		}

		public static NodeConnectionInfo GetLeaderNode(this RaftEngine engine)
		{
			var leader = engine.CurrentLeader;
			if (leader == null)
				return null;

			return engine.CurrentTopology.AllNodes.Single(x => x.Name == leader);
		}

		public static bool IsLeader(this RavenRaftEngine engine)
		{
			return engine.Engine.State == RaftEngineState.Leader;
		}

		public static NodeConnectionInfo GetFirstNonSelfNode(this RaftEngine engine)
		{
			var selfNode = engine.Options.SelfConnection;

			return engine.CurrentTopology.AllNodes.First(x => x.Name != selfNode.Name);
		}

		public static bool IsClusterDatabase(this DatabaseDocument document)
		{
			string value;
			bool result;
			if (document.Settings.TryGetValue(Constants.Cluster.NonClusterDatabaseMarker, out value) == false)
				return true;

			if (bool.TryParse(value, out result) == false) 
				return true;

			if (result) 
				return false;

			return true;
		}

		public static void AssertClusterDatabase(this DatabaseDocument document)
		{
			if (document.IsClusterDatabase() == false)
				throw new InvalidOperationException("Not a cluster database. Database: " + document.Id);
		}

		public static string GetNormalizedNodeUrl(String url)
		{
			return GetNodeUrl(url).AbsoluteUri.ToLower();
		}

		public static Uri GetNodeUrl(string url)
		{
			if (string.IsNullOrEmpty(url))
				throw new ArgumentNullException("url");

			if (url.EndsWith("/") == false && url.EndsWith("\\") == false)
				url += "/";

			return new Uri(url, UriKind.Absolute);
		}

		public static string GetNodeName(Guid name)
		{
			return name.ToString();
		}
	}
}