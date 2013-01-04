using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Exceptions;
using Xunit;

namespace Raven.Tests.Bundles.Replication.Bugs
{
	public class SameEtagFromDifferentServers : ReplicationBase
	{
		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void MakeSureEtagsDiffer()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			using (var session1 = store1.OpenSession())
			{
				session1.Store(new User { Id = "users/ayende", Name = "Ayende" });
				session1.SaveChanges();
			}

			using (var session2 = store2.OpenSession())
			{
				session2.Store(new User { Id = "users/ayende", Name = "Oren" });
				session2.SaveChanges();
				var u = session2.Load<User>("users/ayende");
			}

			TellFirstInstanceToReplicateToSecondInstance();


			for (int i = 0; i < RetriesCount; i++)
			{
				try
				{
					using (var session = store2.OpenSession())
					{
						session.Load<User>("users/ayende");
					}
					Thread.Sleep(100);
				}
				catch (ConflictException)
				{
					break;
				}
			}
			using (var session2 = store2.OpenSession())
			{
				try
				{
					session2.Load<User>("users/ayende");
				}
				catch (ConflictException e)
				{
					var list = new List<JsonDocument>();
					Assert.Equal<int>(2, e.ConflictedVersionIds.Length);

					// Etags have to be different!
					Assert.NotEqual<string>(e.ConflictedVersionIds[0], e.ConflictedVersionIds[1]);
				}
			}
		}
	}
}
