// -----------------------------------------------------------------------
//  <copyright file="HiLoHanging.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Document;
using Raven.Json.Linq;
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

			WaitForReplication(store2, key);

			var etag = store1.DatabaseCommands.Get(key).Etag;

			var jsonDocument = store2.DatabaseCommands.Get(key);
			store2.DatabaseCommands.Put(key, null, new RavenJObject
			{
				{"Max", 49}
			}, jsonDocument.Metadata);

			WaitForReplication(store1, key, changedSince: etag);

			for (long i = 0; i < 4; i++)
			{
				Assert.Equal(i + 50, hiLoKeyGenerator.NextId(store1.DatabaseCommands));
			}
		}
	}
}