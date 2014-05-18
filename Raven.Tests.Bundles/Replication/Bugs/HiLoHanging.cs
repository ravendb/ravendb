// -----------------------------------------------------------------------
//  <copyright file="HiLoHanging.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication.Bugs
{
	public class HiLoHanging : ReplicationBase
	{
		[Fact]
		public void HiLo_Modified_InReplicated_Scenario()
		{
			const string key = "Raven/Hilo/managers";

			var store1 = CreateStore(configureStore: store => store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromAllServers);
			var store2 = CreateStore();

			((DocumentStore) store1).GetReplicationInformerForDatabase()
			                        .UpdateReplicationInformationIfNeeded((ServerClient) store1.DatabaseCommands)
			                        .Wait();
			((DocumentStore) store2).GetReplicationInformerForDatabase()
			                        .UpdateReplicationInformationIfNeeded((ServerClient) store2.DatabaseCommands)
			                        .Wait();

			TellFirstInstanceToReplicateToSecondInstance();
			TellSecondInstanceToReplicateToFirstInstance();

			var hiLoKeyGenerator = new HiLoKeyGenerator("managers", 2)
			{
				DisableCapacityChanges = true
			};
			for (long i = 0; i < 4; i++)
			{
				Assert.Equal(i + 1, hiLoKeyGenerator.NextId(store1.DatabaseCommands));
			}

			WaitForReplication(store2, session =>
			{
				var load = session.Load<dynamic>(key);
				return load != null && load.Max == 4;
			});

			var jsonDocument = store2.DatabaseCommands.Get(key);
			store2.DatabaseCommands.Put(key, null, new RavenJObject
			{
				{"Max", 49}
			}, jsonDocument.Metadata);

			WaitForReplication(store1, session => session.Load<dynamic>(key).Max == 49);

			for (long i = 0; i < 4; i++)
			{
				var nextId = hiLoKeyGenerator.NextId(store1.DatabaseCommands);
				Assert.Equal(i + 50, nextId);
			}
		}
	}
}