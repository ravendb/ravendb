// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2556.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2556 : ReplicationBase
	{
		[Fact]
		public void FailoverBehaviorShouldBeReadFromServer()
		{
			IDocumentStore store1 = null;
			IDocumentStore store2 = null;

			try
			{
				store1 = CreateStore();
				store2 = CreateStore();

				Assert.Equal(FailoverBehavior.AllowReadsFromSecondaries, store1.Conventions.FailoverBehavior);

				RunReplication(store1, store2, clientConfiguration: new ReplicationClientConfiguration { FailoverBehavior = FailoverBehavior.ReadFromAllServers });

				var serverClient = ((ServerClient)store1.DatabaseCommands);
				GetReplicationInformer(serverClient).RefreshReplicationInformation(serverClient);

				Assert.Equal(FailoverBehavior.ReadFromAllServers, store1.Conventions.FailoverBehavior);
			}
			finally
			{
				if (store1 != null)
				{
					var serverClient = ((ServerClient)store1.DatabaseCommands);
					GetReplicationInformer(serverClient).ClearReplicationInformationLocalCache(serverClient);

					store1.Dispose();
				}

				if (store2 != null)
				{
					var serverClient = ((ServerClient)store2.DatabaseCommands);
					GetReplicationInformer(serverClient).ClearReplicationInformationLocalCache(serverClient);

					store2.Dispose();
				}
			}

		}
	}
}