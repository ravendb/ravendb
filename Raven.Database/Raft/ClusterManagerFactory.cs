// -----------------------------------------------------------------------
//  <copyright file="ClusterManagerFactory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

using Rachis;
using Rachis.Commands;
using Rachis.Storage;
using Rachis.Transport;

using Raven.Database.Raft.Storage;
using Raven.Database.Raft.Util;
using Raven.Database.Server.Tenancy;
using Raven.Database.Util;

using Voron;

namespace Raven.Database.Raft
{
	public static class ClusterManagerFactory
	{
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

			var transport = new HttpTransport(nodeConnectionInfo.Name);
			var stateMachine = new ClusterStateMachine(systemDatabase, databasesLandlord);
			var raftEngineOptions = new RaftEngineOptions(nodeConnectionInfo, options, transport, stateMachine)
									{
										ElectionTimeout = 2000,
										HeartbeatTimeout = 750
									};

			if (Debugger.IsAttached)
			{
				raftEngineOptions.ElectionTimeout *= 5;
				raftEngineOptions.HeartbeatTimeout *= 5;
			}

			return new ClusterManager(new RaftEngine(raftEngineOptions));
		}

		public static void InitializeTopology(NodeConnectionInfo nodeConnection, ClusterManager engine)
		{
			var topology = new Topology(Guid.Parse(nodeConnection.Name), new List<NodeConnectionInfo> { nodeConnection }, Enumerable.Empty<NodeConnectionInfo>(), Enumerable.Empty<NodeConnectionInfo>());

			var tcc = new TopologyChangeCommand
					  {
						  Requested = topology
					  };

			engine.Engine.PersistentState.SetCurrentTopology(tcc.Requested, 0);
			engine.Engine.StartTopologyChange(tcc);
			engine.Engine.CommitTopologyChange(tcc);
			engine.Engine.CurrentLeader = null;
		}

		public static void InitializeTopology(ClusterManager engine)
		{
			InitializeTopology(engine.Engine.Options.SelfConnection, engine);
		}
	}
}