// -----------------------------------------------------------------------
//  <copyright file="ClusterManagerFactory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Rachis;
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

        public static ClusterManager Create(DocumentDatabase systemDatabase, DatabasesLandlord databasesLandlord, bool nullifyLastAppliedIndex = false)
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
                options = StorageEnvironmentOptions.CreateMemoryOnly(configuration.Storage.Voron.TempPath);
            }

            var shortOperationsTimeout = TimeSpan.FromMilliseconds(1.5 * Math.Max(configuration.Cluster.ElectionTimeout, configuration.Cluster.HeartbeatTimeout));
            var transport = new HttpTransport(nodeConnectionInfo.Name, shortOperationsTimeout, systemDatabase.WorkContext.CancellationToken);
            var stateMachine = new ClusterStateMachine(systemDatabase, databasesLandlord);
            var raftEngineOptions = new RaftEngineOptions(nodeConnectionInfo, options, transport, stateMachine)
            {
                ElectionTimeout = configuration.Cluster.ElectionTimeout,
                HeartbeatTimeout = configuration.Cluster.HeartbeatTimeout,
                MaxLogLengthBeforeCompaction = configuration.Cluster.MaxLogLengthBeforeCompaction,
                MaxEntriesPerRequest = configuration.Cluster.MaxEntriesPerRequest,
                MaxStepDownDrainTime = configuration.Cluster.MaxStepDownDrainTime,
            };
            var raftEngine = new RaftEngine(raftEngineOptions);
            stateMachine.RaftEngine = raftEngine;           
            return new ClusterManager(raftEngine, databasesLandlord);
        }
    }
}
