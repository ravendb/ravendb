// -----------------------------------------------------------------------
//  <copyright file="Indexes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Indexes;
using Raven.Tests.Common.Dto;

using Xunit.Extensions;

namespace Raven.Tests.Raft.Client
{
	public class Indexes : RaftTestBase
	{
		private class Test_Index : AbstractIndexCreationTask<Person>
		{
			public Test_Index()
			{
				Map = persons => from person in persons select new { Name = person.Name };
			}
		}

		[Theory]
		[PropertyData("Nodes")]
		public void PutAndDeleteShouldBePropagated(int numberOfNodes)
		{
			throw new NotImplementedException("Waiting for http://issues.hibernatingrhinos.com/issue/RavenDB-3304");

			var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			SetupClusterConfiguration(clusterStores);

			var store1 = clusterStores[0];
			store1.DatabaseCommands.PutIndex("Test/Index", new Test_Index().CreateIndexDefinition(), true);

			var requestFactory = new HttpRavenRequestFactory();
			var replicationRequestUrl = string.Format("{0}/replication/replicate-indexes?op=replicate-all", store1.Url.ForDatabase(store1.DefaultDatabase));
			if (numberOfNodes > 1)
			{
				var replicationRequest = requestFactory.Create(replicationRequestUrl, "POST", new RavenConnectionStringOptions { Url = store1.Url });
				replicationRequest.ExecuteRequest();
			}

			clusterStores.ForEach(store => WaitFor(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), commands => commands.GetIndex("Test/Index") != null, TimeSpan.FromMinutes(1)));

			store1.DatabaseCommands.DeleteIndex("Test/Index");

			if (numberOfNodes > 1)
			{
				var replicationRequest = requestFactory.Create(replicationRequestUrl, "POST", new RavenConnectionStringOptions { Url = store1.Url });
				replicationRequest.ExecuteRequest();
			}

			clusterStores.ForEach(store => WaitFor(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), commands => commands.GetIndex("Test/Index") == null, TimeSpan.FromMinutes(1)));
		}
	}
}