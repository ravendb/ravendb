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
			using (var session_for_storing = documentStore.OpenAsyncSession())
			{
				session_for_storing.Store(entity1);
				session_for_storing.Store(entity2);
				var result = session_for_storing.SaveChangesAsync();
				EnqueueConditional(() => result.IsCompleted || result.IsFaulted);
			}

			EnqueueCallback(() =>
			{
				using (var session_for_loading = documentStore.OpenAsyncSession())
				{
					var task = session_for_loading.MultiLoadAsync<Company>(new[] { entity1.Id, entity2.Id });
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

		[Asynchronous]
		[TestMethod]
		public void Can_insert_async_and_load_async()
		{
			var documentStore = new DocumentStore { Url = "http://localhost:" + port };
			documentStore.Initialize();

			var entity = new Company { Name = "Async Company #1" };
			using (var session_for_storing = documentStore.OpenAsyncSession())
			{
				session_for_storing.Store(entity);
				var result = session_for_storing.SaveChangesAsync();
				EnqueueConditional(() => result.IsCompleted || result.IsFaulted);
			}

			EnqueueCallback(() =>
			{
				using (var session_for_loading = documentStore.OpenAsyncSession())
				{
					var task = session_for_loading.LoadAsync<Company>(entity.Id);
					EnqueueConditional(() => task.IsCompleted || task.IsFaulted);
					EnqueueCallback(() => Assert.Equal(entity.Name, task.Result.Name));
				}
				EnqueueTestComplete();
			});
		}

		[Asynchronous]
		[TestMethod]
		public void Can_insert_async_and_delete_async()
		{
			var documentStore = new DocumentStore { Url = "http://localhost:" + port };
			documentStore.Initialize();

			var entity = new Company { Name = "Async Company #1" };
			using (var session = documentStore.OpenAsyncSession())
			{
				session.Store(entity);
				var storing = session.SaveChangesAsync();
				EnqueueConditional(() => storing.IsCompleted || storing.IsFaulted);

				EnqueueCallback(()=>
				                	{
										using (var for_loading = documentStore.OpenAsyncSession())
										{
											var loading =for_loading.LoadAsync<Company>(entity.Id);
											EnqueueConditional(() => loading.IsCompleted || loading.IsFaulted);
											EnqueueCallback(() => Assert.NotNull(loading.Result));
										}
				                	});

				EnqueueCallback(() =>
				{
				    session.Delete(entity);
				    var deleting = session.SaveChangesAsync();
					EnqueueConditional(() => deleting.IsCompleted || deleting.IsFaulted);
					EnqueueCallback(() =>
					                	{
					                		using (var for_verifying = documentStore.OpenAsyncSession())
					                		{
					                			var verification = for_verifying.LoadAsync<Company>(entity.Id);
					                			EnqueueConditional(() => verification.IsCompleted || verification.IsFaulted);
					                			EnqueueCallback(() => Assert.Null(verification.Result));
					                			EnqueueTestComplete();
					                		}
					                	});
				});
			}
		}

		[Asynchronous]
		[TestMethod]
		public void Can_get_index_names_async()
		{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				var task = documentStore.AsyncDatabaseCommands.GetIndexNamesAsync(0,25);
				EnqueueConditional(() => task.IsCompleted || task.IsFaulted);
				EnqueueCallback(()=> Assert.Equal(new[] { "Raven/DocumentsByEntityName" },task.Result));
				EnqueueTestComplete();
		}
	}
}