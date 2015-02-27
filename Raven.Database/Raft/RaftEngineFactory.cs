// -----------------------------------------------------------------------
//  <copyright file="RaftEngineFactory.cs" company="Hibernating Rhinos LTD">
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

			var nodeName = systemDatabase.TransactionalStorage.Id.ToString();

			var url = configuration.ServerUrl;
			if (string.IsNullOrEmpty(configuration.DatabaseName) == false)
				url = url.ForDatabase(configuration.DatabaseName);

			var nodeConnectionInfo = new NodeConnectionInfo
			{
				Name = nodeName,
				Uri = RaftHelper.GetNodeUrl(url)
			};

			var directoryPath = Path.Combine(configuration.DataDirectory ?? AppDomain.CurrentDomain.BaseDirectory, "Raft");
			if (Directory.Exists(directoryPath) == false)
				Directory.CreateDirectory(directoryPath);

			var stateMachine = new InMemoryStateMachine();

			var options = StorageEnvironmentOptions.ForPath(directoryPath);
			var transport = new HttpTransport(nodeName);

			var raftEngineOptions = new RaftEngineOptions(nodeConnectionInfo, options, transport, stateMachine) { HeartbeatTimeout = 1000 };

			return new RaftEngine(raftEngineOptions);
		}

		public static void InitializeTopology(DocumentDatabase systemDatabase, RaftEngine raftEngine)
		{
			var topology = new Topology(systemDatabase.TransactionalStorage.Id, new List<NodeConnectionInfo> { raftEngine.Options.SelfConnection }, Enumerable.Empty<NodeConnectionInfo>(), Enumerable.Empty<NodeConnectionInfo>());

			raftEngine.StartTopologyChange(new TopologyChangeCommand { Requested = topology });
		}
	}
}