namespace Raven.Tests.Issues
{
	using System;
	using System.Threading;

	using Raven.Bundles.Tests.Replication;
	using Raven.Client;
	using Raven.Client.Document;
	using Raven.Client.Exceptions;

	using Xunit;
	using Xunit.Sdk;

	public class RavenDB_689 : ReplicationBase
	{
		public class User
		{
			public long Tick { get; set; }
		}

		public RavenDB_689()
		{
			this.RetriesCount = 100;
		}

		[Fact]
		public void Test1()
		{
			var store1 = this.CreateStore();
			var store2 = this.CreateStore();
			var store3 = this.CreateStore();

			this.SetupReplication(store1.DatabaseCommands, store2.Url, store3.Url);
			this.SetupReplication(store2.DatabaseCommands, store1.Url, store3.Url);

			using (var session = store1.OpenSession())
			{
				session.Store(new User { Tick = 1 });
				session.SaveChanges();
			}

			this.WaitForDocument(store2.DatabaseCommands, "users/1");
			this.WaitForDocument(store3.DatabaseCommands, "users/1");

			this.RemoveReplication(store1.DatabaseCommands);
			this.RemoveReplication(store2.DatabaseCommands);

			this.SetupReplication(store2.DatabaseCommands, store3.Url);

			using (var session = store2.OpenSession())
			{
				var user = session.Load<User>("users/1");
				user.Tick = 2;
				session.Store(user);
				session.SaveChanges();
			}

			Thread.Sleep(3000);

			Assert.Equal(1, this.WaitForDocument<User>(store1, "users/1").Tick);
			Assert.Equal(2, this.WaitForDocument<User>(store3, "users/1").Tick);

			using (var session = store1.OpenSession())
			{
				var user = session.Load<User>("users/1");
				user.Tick = 3;
				session.Store(user);
				session.SaveChanges();
			}

			this.RemoveReplication(store2.DatabaseCommands);
			this.SetupReplication(store1.DatabaseCommands, store3.Url);

			var conflictException = Assert.Throws<ConflictException>(() =>
			{
				for (int i = 0; i < RetriesCount; i++)
				{
					using (var session = store3.OpenSession())
					{
						session.Load<User>("users/1");
						Thread.Sleep(100);
					}
				}
			});

			Assert.Equal("Conflict detected on users/1, conflict must be resolved before the document will be accessible", conflictException.Message);

			this.RemoveReplication(store1.DatabaseCommands);
			this.RemoveReplication(store2.DatabaseCommands);
			this.SetupReplication(store1.DatabaseCommands, store2.Url, store3.Url);
			this.SetupReplication(store2.DatabaseCommands, store1.Url, store3.Url);

			IDocumentStore store;

			try
			{
				conflictException = Assert.Throws<ConflictException>(
					() =>
					{
						for (int i = 0; i < RetriesCount; i++)
						{
							using (var session = store1.OpenSession())
							{
								session.Load<User>("users/1");
								Thread.Sleep(100);
							}
						}
					});

				store = store1;
			}
			catch (ThrowsException)
			{
				conflictException = Assert.Throws<ConflictException>(
					() =>
					{
						for (int i = 0; i < RetriesCount; i++)
						{
							using (var session = store2.OpenSession())
							{
								session.Load<User>("users/1");
								Thread.Sleep(100);
							}
						}
					});

				store = store2;
			}

			Assert.Equal("Conflict detected on users/1, conflict must be resolved before the document will be accessible", conflictException.Message);

			long expectedTick = -1;

			try
			{
				store.DatabaseCommands.Get("users/1");
			}
			catch (ConflictException e)
			{
				var c1 = store.DatabaseCommands.Get(e.ConflictedVersionIds[0]);
				var c2 = store.DatabaseCommands.Get(e.ConflictedVersionIds[1]);

				store.DatabaseCommands.Put("users/1", null, c1.DataAsJson, c1.Metadata);

				expectedTick = long.Parse(c1.DataAsJson["Tick"].ToString());
			}

			Thread.Sleep(3000);

			Assert.Equal(expectedTick, this.WaitForDocument<User>(store1, "users/1").Tick);
			Assert.Equal(expectedTick, this.WaitForDocument<User>(store2, "users/1").Tick);
			Assert.Equal(expectedTick, this.WaitForDocument<User>(store3, "users/1").Tick);
		}
	}
}
