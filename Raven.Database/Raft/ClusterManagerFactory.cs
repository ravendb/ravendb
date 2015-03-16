// -----------------------------------------------------------------------
//  <copyright file="ClusterManagerFactory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Rachis;
using Rachis.Commands;
using Rachis.Storage;
using Rachis.Transport;

using Raven.Abstractions.Logging;
using Raven.Database.Raft.Storage;
using Raven.Database.Raft.Util;
using Raven.Database.Server.Tenancy;
using Raven.Database.Util;

using Voron;

namespace Raven.Database.Raft
{
	public static class ClusterManagerFactory
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		public static NodeConnectionInfo CreateSelfConnection(DocumentDatabase database)
		{
			var configuration = database.Configuration;

			var nodeName = RaftHelper.GetNodeName(database.TransactionalStorage.Id);

			var url = configuration.ServerUrl;

			return new NodeConnectionInfo
			{
				Name = nodeName,
				Uri = RaftHelper.GetNodeUrl(url)
			};
		}

		public static ClusterManager Create(DocumentDatabase systemDatabase, DatabasesLandlord databasesLandlord)
		{
			if (systemDatabase == null)
				throw new ArgumentNullException("systemDatabase");

			if (databasesLandlord == null)
				throw new ArgumentNullException("databasesLandlord");

			DatabaseHelper.AssertSystemDatabase(systemDatabase);

			var configuration = systemDatabase.Configuration;
			var nodeConnectionInfo = CreateSelfConnection(systemDatabase);

			StorageEnvironmentOptions options;
			if (configuration.RunInMemory == false)
			{
				var directoryPath = Path.Combine(configuration.DataDirectory ?? AppDomain.CurrentDomain.BaseDirectory, "Raft");
				if (Directory.Exists(directoryPath) == false)
					Directory.CreateDirectory(directoryPath);

				options = StorageEnvironmentOptions.ForPath(directoryPath);
			}
			else
			{
				options = StorageEnvironmentOptions.CreateMemoryOnly();
			}

			var transport = new HttpTransport(nodeConnectionInfo.Name, systemDatabase.WorkContext.CancellationToken);
			var stateMachine = new ClusterStateMachine(systemDatabase, databasesLandlord);
			var raftEngineOptions = new RaftEngineOptions(nodeConnectionInfo, options, transport, stateMachine)
			{
				ElectionTimeout = configuration.Cluster.ElectionTimeout,
				HeartbeatTimeout = configuration.Cluster.HeartbeatTimeout,
				MaxLogLengthBeforeCompaction = configuration.Cluster.MaxLogLengthBeforeCompaction,
				MaxEntriesPerRequest = configuration.Cluster.MaxEntriesPerRequest,
				MaxStepDownDrainTime = configuration.Cluster.MaxStepDownDrainTime
			};
			var raftEngine = new RaftEngine(raftEngineOptions);
			stateMachine.RaftEngine = raftEngine;

			return new ClusterManager(raftEngine);
		}

		public static void InitializeTopology(NodeConnectionInfo nodeConnection, ClusterManager clusterManager)
		{
			var topologyId = Guid.Parse(nodeConnection.Name);
			var topology = new Topology(topologyId, new List<NodeConnectionInfo> { nodeConnection }, Enumerable.Empty<NodeConnectionInfo>(), Enumerable.Empty<NodeConnectionInfo>());

			var tcc = new TopologyChangeCommand
					  {
						  Requested = topology
					  };

			clusterManager.Engine.PersistentState.SetCurrentTopology(tcc.Requested, 0);
			clusterManager.Engine.StartTopologyChange(tcc);
			clusterManager.Engine.CommitTopologyChange(tcc);
			clusterManager.Engine.CurrentLeader = null;

			Log.Info("Initialized Topology: " + topologyId);
		}

		public static void InitializeTopology(ClusterManager engine)
		{
			InitializeTopology(engine.Engine.Options.SelfConnection, engine);
		}
	}
}