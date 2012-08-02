using System.Linq;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class BuildStarted : RavenTest
	{
		public class TestModel
		{
			public int Id { get; set; }
		}

		//This test attempts to lookup an existing item when none exist in the
		//database. It returns null which that variable is assigned a new value
		//storing that new value, however, fails.
		[Fact]
		public void DocumentStoreFailsWhenGrabbingNonExistingItemAndStoringNewOne()
		{
			using (var documentStore = new EmbeddableDocumentStore { RunInMemory = true, Identifier = "docstore1" }.Initialize())
			{
				documentStore.Conventions.AllowQueriesOnId = true;
				using (var session = documentStore.OpenSession())
				{
					TestModel testModelItem = session.Query<TestModel>().SingleOrDefault(t => t.Id == 1) ??
											  new TestModel { Id = 1 };
					Assert.NotNull(testModelItem);
					session.Store(testModelItem);
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var list = session.Query<TestModel>()
						.Customize(x=>x.WaitForNonStaleResults())
						.ToList();
					Assert.Equal(1, list.Count());
				}
			}
		}


		//This test adds a new item to the database and then performs a lookup on a
		//non existing item. Sets the variable to a new value and stores it.
		//works like expected
		[Fact]
		public void DocumentStoreWorksWhenAddingItemAndThenGrabbingNonExistingItemAndStoringNewOne()
		{
			using (var documentStore = new EmbeddableDocumentStore { RunInMemory = true, Identifier = "docstore2" }.Initialize())
			{
				documentStore.Conventions.AllowQueriesOnId = true; 
				using (var session = documentStore.OpenSession())
				{
					session.Store(new TestModel { Id = 1 });
					session.SaveChanges();

					TestModel testModelItem = session.Query<TestModel>().SingleOrDefault(t => t.Id == 2) ??
												  new TestModel { Id = 2 };
					Assert.NotNull(testModelItem);
					session.Store(testModelItem);
					session.SaveChanges();

					var list = session.Query<TestModel>()
						.Customize(x=>x.WaitForNonStaleResults())
						.ToList();
					Assert.Equal(2, list.Count());
				}
			}
		}

		//This test adds a new item to the database and then deletes it and then
		//performs a lookup on a non existing item. Sets the variable to a new 
		//value and stores it, however, fails.
		[Fact]
		public void DocumentStoreWorksWhenAddingItemThenDeletingItAndThenGrabbingNonExistingItemAndStoringNewOne()
		{
			using (var documentStore = new EmbeddableDocumentStore { RunInMemory = true, Identifier = "docstore3" }.Initialize())
			{
				documentStore.Conventions.AllowQueriesOnId = true;
				using (var session = documentStore.OpenSession())
				{
					var deletedModel = new TestModel { Id = 1 };
					session.Store(deletedModel);
					session.SaveChanges();

					session.Delete(deletedModel);
					session.SaveChanges();

					TestModel testModelItem = session.Query<TestModel>().SingleOrDefault(t => t.Id == 2) ??
												  new TestModel { Id = 2 };
					Assert.NotNull(testModelItem);
					session.Store(testModelItem);
					session.SaveChanges();

					var list = session.Query<TestModel>()
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();
					Assert.Equal(1, list.Count());
				}
			}
		}
	}
}