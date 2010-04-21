using Raven.Database;
using Xunit;

namespace Raven.Tests.Storage
{
	public class GeneralStorage : AbstractDocumentStorageTest
	{

		private readonly DocumentDatabase db;

		public GeneralStorage()
		{
			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = "raven.db.test.esent"});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanGetDocumentCounts()
		{
			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(0, actions.GetDocumentsCount());

				actions.AddDocument("a", "b", null, "a");

				actions.Commit();
			});

			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(1, actions.GetDocumentsCount());

				actions.DeleteDocument("a", null);

				actions.Commit();
			});


			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(0, actions.GetDocumentsCount());

				actions.Commit();
			});
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