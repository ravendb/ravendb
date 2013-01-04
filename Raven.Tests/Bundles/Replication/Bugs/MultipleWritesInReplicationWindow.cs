using System.Threading;
using Xunit;

namespace Raven.Tests.Bundles.Replication.Bugs
{
	public class MultipleWritesInReplicationWindow : ReplicationBase
	{
		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void WriteMultipleTimesToDocumentFast()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			using (var session1 = store1.OpenSession())
			{
				session1.Store(new User { Id = "users/ayende", Name = "Ayende" });
				session1.SaveChanges();
			}

			TellFirstInstanceToReplicateToSecondInstance();


			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store2.OpenSession())
				{
					if (session.Load<User>("users/ayende") != null)
						break;
				}
				Thread.Sleep(100);
			}

			for (int i = 0; i < 19; i++)
			{
				using (var session1 = store1.OpenSession())
				{
					session1.Load<User>("users/ayende").Name += "Ayende #" + i;
					session1.SaveChanges();
				}
			}

			string name;
			using (var session1 = store1.OpenSession())
			{
				var userThatWouldBeReplicated = session1.Load<User>("users/ayende");
				name = userThatWouldBeReplicated.Name;
			}

			User user = null;
			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session2 = store2.OpenSession())
				{
					user = session2.Load<User>("users/ayende");
					if (user != null)
						if (user.Name == name)
							break;
				}
				Thread.Sleep(100);
			}

			Assert.Equal(user.Name, name);

		}
	}
}