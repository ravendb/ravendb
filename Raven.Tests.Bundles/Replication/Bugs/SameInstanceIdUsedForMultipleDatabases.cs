// -----------------------------------------------------------------------
//  <copyright file="Samuel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication.Bugs
{
	public class SameInstanceIdUsedForMultipleDatabases : ReplicationBase
	{
		[Fact]
		public void EachReplicatedDatabaseShouldHaveADifferentServerInstanceId()
		{
			var master1 = CreateStore();
			var master2 = CreateStore();
			var slave = CreateStore();

			TellInstanceToReplicateToAnotherInstance(0, 2);
			TellInstanceToReplicateToAnotherInstance(1, 2);

			using (var session1 = master1.OpenSession())
			{
				session1.Store(new { Test = "Test" }, "Doc1");
				session1.SaveChanges();
			}

			using (var session2 = master2.OpenSession())
			{
				session2.Store(new { Test = "Test" }, "Doc2");
				session2.SaveChanges();
			}

			WaitForDocument(slave.DatabaseCommands, "Doc2");
			WaitForDocument(slave.DatabaseCommands, "Doc1");

			var docs = slave.DatabaseCommands.StartsWith("Raven/Replication/Sources", "", 0, 10);

			var serverInstanceIds = docs.Select(d => d.DataAsJson["ServerInstanceId"]).Distinct().ToList();

			Assert.Equal(2, serverInstanceIds.Count);
		}
	}
}