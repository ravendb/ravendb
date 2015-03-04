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
			if (document.Settings.TryGetValue(Constants.Cluster.ClusterDatabaseMarker, out value) && bool.TryParse(value, out result))
				return result;

			return false;
		}

		public static void AssertClusterDatabase(this DatabaseDocument document)
		{
			if (document.IsClusterDatabase() == false)
				throw new InvalidOperationException("Not a cluster database. Database: " + document.Id);
		}

		public static Uri GetNodeUrl(string url)
		{
			if (string.IsNullOrEmpty(url))
				throw new ArgumentNullException("url");

			if (url.EndsWith("/") == false && url.EndsWith("\\") == false)
				url += "/";

			return new Uri(url, UriKind.Absolute);
		}

		public static string GetDatabaseKey(string key)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentNullException("url");

			if (key.StartsWith(Constants.RavenDatabasesPrefix, StringComparison.OrdinalIgnoreCase))
				return key;

			return Constants.RavenDatabasesPrefix + key;
		}

		public static string GetDatabaseName(string key)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentNullException("url");

			if (key.StartsWith(Constants.RavenDatabasesPrefix, StringComparison.OrdinalIgnoreCase))
				return key.Substring(Constants.RavenDatabasesPrefix.Length);

			return key;
		}

		public static string GetNodeName(Guid name)
		{
			return name.ToString();
		}
	}
}