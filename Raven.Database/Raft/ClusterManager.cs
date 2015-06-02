// -----------------------------------------------------------------------
//  <copyright file="ClusterManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Rachis;
using Rachis.Transport;

using Raven.Database.Impl;

namespace Raven.Database.Raft
{
	public class ClusterManager : IDisposable
	{
		public RaftEngine Engine { get; private set; }

		public ClusterManagementHttpClient Client { get; private set; }

		public ClusterManager(RaftEngine engine)
		{
			Engine = engine;
			Client = new ClusterManagementHttpClient(engine);
		}

		public ClusterTopology GetTopology()
		{
			return new ClusterTopology
				   {
					   CurrentLeader = Engine.CurrentLeader,
					   CurrentTerm = Engine.PersistentState.CurrentTerm,
					   State = Engine.State.ToString(),
					   CommitIndex = Engine.CommitIndex,
					   AllVotingNodes = Engine.CurrentTopology.AllVotingNodes.ToArray(),
					   PromotableNodes = Engine.CurrentTopology.PromotableNodes.ToArray(),
					   NonVotingNodes = Engine.CurrentTopology.NonVotingNodes.ToArray(),
					   TopologyId = Engine.CurrentTopology.TopologyId
				   };
		}

		public void Dispose()
		{
			var aggregator = new ExceptionAggregator("ClusterManager disposal error.");

			aggregator.Execute(() =>
			{
				if (Client != null)
					Client.Dispose();
			});

			aggregator.Execute(() =>
			{
				if (Engine != null)
					Engine.Dispose();
			});

			aggregator.ThrowIfNeeded();
		}
	}

	public class ClusterTopology
	{
		public string CurrentLeader { get; set; }

		public long CurrentTerm { get; set; }

		public string State { get; set; }

		public long CommitIndex { get; set; }

		public NodeConnectionInfo[] AllVotingNodes { get; set; }

		public NodeConnectionInfo[] PromotableNodes { get; set; }

		public NodeConnectionInfo[] NonVotingNodes { get; set; }

		public Guid TopologyId { get; set; }
	}
}