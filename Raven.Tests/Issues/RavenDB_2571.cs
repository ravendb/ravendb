// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2571.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;

using Raven.Client.Connection;
using Raven.Client.Exceptions;
using Raven.Tests.Bugs;
using Raven.Tests.Bundles.Replication;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2571 : ReplicationBase
	{
		[Fact]
		public void ThereShouldBeOnlyOneConflictPerReplicationSource()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				TellFirstInstanceToReplicateToSecondInstance();

				using (var session = store1.OpenSession())
				{
					session.Store(new User
								  {
									  Name = "Name1"
								  });

					session.SaveChanges();
				}

				WaitForDocument<User>(store2, "users/1");

				using (var session = store2.OpenSession())
				{
					var user = session.Load<User>("users/1");
					user.Name = "Name2";

					session.SaveChanges();
				}

				using (var session = store1.OpenSession())
				{
					var user = session.Load<User>("users/1");
					user.Name = "Name3";

					session.SaveChanges();
				}

				Assert.Throws<ConflictException>(() => WaitForReplication(store2, session => session.Load<User>("users/1").Name == "Name3"));

				using (var session = store1.OpenSession())
				{
					var user = session.Load<User>("users/1");
					user.Name = "Name4";

					session.SaveChanges();
				}

				var conflictOccured = false;
				for (var i = 0; i < 10; i++)
				{
					try
					{
						using (var session = store2.OpenSession())
						{
							var user = session.Load<User>("users/1");
							Assert.True(false);
						}
					}
					catch (ConflictException e)
					{
						conflictOccured = true;

						Assert.Equal(2, e.ConflictedVersionIds.Length);

						var doc1 = store2.DatabaseCommands.Get(e.ConflictedVersionIds[0]);
						var doc2 = store2.DatabaseCommands.Get(e.ConflictedVersionIds[1]);

						var user1 = doc1.DataAsJson.Deserialize<User>(store2.Conventions);
						var user2 = doc2.DataAsJson.Deserialize<User>(store2.Conventions);

						if (user1.Name == "Name4" || user2.Name == "Name4")
							break;
					}

					Thread.Sleep(1000);
				}

				Assert.True(conflictOccured);
			}
		}
	}
}