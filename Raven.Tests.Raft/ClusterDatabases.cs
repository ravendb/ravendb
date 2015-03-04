// -----------------------------------------------------------------------
//  <copyright file="ClusterDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;

using Xunit.Extensions;

namespace Raven.Tests.Raft
{
	public class ClusterDatabases : RaftTestBase
	{
		[Theory]
		[InlineData(1)]
		[InlineData(3)]
		[InlineData(5)]
		[InlineData(11)]
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
						                                                   {"Raven/DataDir", "~/Databases/Northwind"},
						                                                   {Constants.Cluster.ClusterDatabaseMarker, "true"}
					                                                   }
																   });

				var key = Constants.RavenDatabasesPrefix + "Northwind";

				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), key));
			}
		}

		[Theory]
		[InlineData(1)]
		[InlineData(3)]
		[InlineData(5)]
		[InlineData(11)]
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
						{Constants.Cluster.ClusterDatabaseMarker, "true"}
					}
				});

				var key = Constants.RavenDatabasesPrefix + "Northwind";

				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), key));

				store1.DatabaseCommands.GlobalAdmin.DeleteDatabase(key);

				clusterStores.ForEach(store => WaitForDelete(store.DatabaseCommands.ForSystemDatabase(), key));
			}
		}
	}
}