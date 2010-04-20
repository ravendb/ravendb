using Raven.Database;
using Xunit;

namespace Raven.Tests.Storage
{
	public class Identity : AbstractDocumentStorageTest
	{

		private readonly DocumentDatabase db;

		public Identity()
		{
			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = "raven.db.test.esent"});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanGetNewIdentityValues()
		{
			db.TransactionalStorage.Batch(actions=>
			{
				var nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(1, nextIdentityValue);

				nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(2, nextIdentityValue);

				actions.Commit();
			});

			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(3, nextIdentityValue);

				nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(4, nextIdentityValue);

				actions.Commit();
			});
		}

		[Fact]
		public void CanGetNewIdentityValuesWhenUsingTwoDifferentItems()
		{
			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(1, nextIdentityValue);

				nextIdentityValue = actions.GetNextIdentityValue("blogs");

				Assert.Equal(1, nextIdentityValue);

				actions.Commit();
			});

			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.GetNextIdentityValue("blogs");

				Assert.Equal(2, nextIdentityValue);

				nextIdentityValue = actions.GetNextIdentityValue("users");

				Assert.Equal(2, nextIdentityValue);

				actions.Commit();
			});
		}
	}
}