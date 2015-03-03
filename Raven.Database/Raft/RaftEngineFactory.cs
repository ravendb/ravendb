// -----------------------------------------------------------------------
//  <copyright file="RaftEngineFactory.cs" company="Hibernating Rhinos LTD">
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

using Raven.Client.Connection;
using Raven.Database.Raft.Storage;
using Raven.Database.Raft.Util;

using Voron;

namespace Raven.Database.Raft
{
	public static class RaftEngineFactory
	{
		public static RaftEngine Create(DocumentDatabase systemDatabase)
		{
			var configuration = systemDatabase.Configuration;

			var nodeName = RaftHelper.GetNodeName(systemDatabase.TransactionalStorage.Id);

			var url = configuration.ServerUrl;
			if (string.IsNullOrEmpty(configuration.DatabaseName) == false)
				url = url.ForDatabase(configuration.DatabaseName);

			var nodeConnectionInfo = new NodeConnectionInfo
			{
				Name = nodeName,
				Uri = RaftHelper.GetNodeUrl(url)
			};

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

			var transport = new HttpTransport(nodeName);
			var stateMachine = new ClusterStateMachine(systemDatabase);
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

			return new RaftEngine(raftEngineOptions);
		}

		public static void InitializeTopology(NodeConnectionInfo nodeConnection, RaftEngine engine)
		{
			var topology = new Topology(Guid.Parse(nodeConnection.Name), new List<NodeConnectionInfo> { nodeConnection }, Enumerable.Empty<NodeConnectionInfo>(), Enumerable.Empty<NodeConnectionInfo>());

			var tcc = new TopologyChangeCommand
					  {
						  Requested = topology
					  };

			engine.PersistentState.SetCurrentTopology(tcc.Requested, 0);
			engine.StartTopologyChange(tcc);
			engine.CommitTopologyChange(tcc);
			engine.CurrentLeader = null;
		}

		public static void InitializeTopology(DocumentDatabase systemDatabase, RaftEngine engine)
		{
			InitializeTopology(engine.Options.SelfConnection, engine);
		}
	}
}