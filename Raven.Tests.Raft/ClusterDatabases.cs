// -----------------------------------------------------------------------
//  <copyright file="ClusterDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Abstractions.Data;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Raft
{
	public class ClusterDatabases : RaftTestBase
	{
		[Theory]
		[PropertyData("Nodes")]
		public void DatabaseShouldBeCreatedOnAllNodes(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes);

			using (var store1 = clusterStores[0])
			{
				store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
																   {
																	   Id = "Northwind",
																	   Settings =
					                                                   {
						                                                   {"Raven/DataDir", "~/Databases/Northwind"}
					                                                   }
																   });

				var key = Constants.RavenDatabasesPrefix + "Northwind";

				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), key));
			}
		}

		[Theory]
		[PropertyData("Nodes")]
		public void DatabaseShouldBeDeletedOnAllNodes(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes);

			using (var store1 = clusterStores[0])
			{
				store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				{
					Id = "Northwind",
					Settings =
					{
						{"Raven/DataDir", "~/Databases/Northwind"},
						{Constants.Cluster.NonClusterDatabaseMarker, "false"}
					}
				});

				var key = Constants.RavenDatabasesPrefix + "Northwind";

				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), key));

				store1.DatabaseCommands.GlobalAdmin.DeleteDatabase(key);

				clusterStores.ForEach(store => WaitForDelete(store.DatabaseCommands.ForSystemDatabase(), key));
			}
		}

		[Fact]
		public void NonClusterDatabasesShouldNotBeCreatedOnAllNodes()
		{
			var clusterStores = CreateRaftCluster(3);

			using (var store1 = clusterStores[0])
			using (var store2 = clusterStores[1])
			using (var store3 = clusterStores[2])
			{
				store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				{
					Id = "Northwind",
					Settings =
					{
						{"Raven/DataDir", "~/Databases/Northwind"},
						{Constants.Cluster.NonClusterDatabaseMarker, "true"}
					}
				});

				var key = Constants.RavenDatabasesPrefix + "Northwind";

				Assert.NotNull(store1.DatabaseCommands.ForSystemDatabase().Get(key));

				var e = Assert.Throws<Exception>(() => WaitForDocument(store2.DatabaseCommands.ForSystemDatabase(), key, TimeSpan.FromSeconds(10)));
				Assert.Equal("WaitForDocument failed", e.Message);

				e = Assert.Throws<Exception>(() => WaitForDocument(store3.DatabaseCommands.ForSystemDatabase(), key, TimeSpan.FromSeconds(10)));
				Assert.Equal("WaitForDocument failed", e.Message);
			}
		}
	}
}