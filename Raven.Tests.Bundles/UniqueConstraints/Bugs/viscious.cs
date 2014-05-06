using Raven.Client.UniqueConstraints;

using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
	public class viscious : UniqueConstraintsTest
	{
		[Fact]
		public void StoreUserWithOneUniqueConstraint()
		{
			var ravenSession = DocumentStore.OpenSession();

			var testUser1 = new TestUser
				{
					Name = "Test User",
					Username = "test@example.com",
					EmailAddres = null
				};
			ravenSession.Store(testUser1);
			ravenSession.SaveChanges();

			var testUser2 = new TestUser
				{
					Name = "Test User2",
					Username = "test@example.com",
					EmailAddres = null
				};

			//this line throws a null reference exception
			var checkResult = ravenSession.CheckForUniqueConstraints(testUser2);

			Assert.False(checkResult.ConstraintsAreFree());

			ravenSession.Dispose();
		}

		public class TestUser
		{
			public string Id { get; set; }
			public string Name { get; set; }

			[UniqueConstraint]
			public string EmailAddres { get; set; }

			[UniqueConstraint]
			public string Username { get; set; }
		}
	}
}

