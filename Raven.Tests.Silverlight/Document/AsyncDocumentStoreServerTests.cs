namespace Raven.Tests.Silverlight.Document
{
	using Client.Document;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;
	using Tests.Document;
	using Assert = Xunit.Assert;

	[TestClass]
	public class AsyncDocumentStoreServerTests : SilverlightTest
	{
		readonly int port = 8080;

		[Asynchronous]
		[TestMethod]
		public void Can_insert_async_and_multi_get_async()
		{
			var documentStore = new DocumentStore {Url = "http://localhost:" + port};
			documentStore.Initialize();

			var entity1 = new Company {Name = "Async Company #1"};
			var entity2 = new Company {Name = "Async Company #2"};
			using (var session1 = documentStore.OpenAsyncSession())
			{
				session1.Store(entity1);
				session1.Store(entity2);
				var result = session1.SaveChangesAsync();
				EnqueueConditional(() => result.IsCompleted || result.IsFaulted);

				EnqueueCallback(() =>
				{
				    using (var session = documentStore.OpenAsyncSession())
				    {
				        var task = session.MultiLoadAsync<Company>(new[] {entity1.Id, entity2.Id});
				        EnqueueConditional(() => task.IsCompleted || task.IsFaulted);
				        EnqueueCallback(() =>
				                			{
				                			    Assert.Equal(entity1.Name, task.Result[0].Name);
				                			    Assert.Equal(entity2.Name, task.Result[1].Name);
				                			});
				    }
				    EnqueueTestComplete();
				});
			}
		}

		[Asynchronous]
		[TestMethod]
		public void Can_insert_async_and_load_async()
		{
			var documentStore = new DocumentStore { Url = "http://localhost:" + port };
			documentStore.Initialize();

			var entity = new Company { Name = "Async Company #1" };
			using (var session1 = documentStore.OpenAsyncSession())
			{
				session1.Store(entity);
				var result = session1.SaveChangesAsync();
				EnqueueConditional(() => result.IsCompleted || result.IsFaulted);

				EnqueueCallback(() =>
				{
					using (var session = documentStore.OpenAsyncSession())
					{
						var task = session.LoadAsync<Company>(entity.Id);
						EnqueueConditional(() => task.IsCompleted || task.IsFaulted);
						EnqueueCallback(() => Assert.Equal(entity.Name, task.Result.Name));
					}
					EnqueueTestComplete();
				});
			}
		}
	}
}