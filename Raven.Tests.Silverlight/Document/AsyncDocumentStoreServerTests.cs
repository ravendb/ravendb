using System.Threading.Tasks;

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
		readonly string url = "http://localhost:";

		[Asynchronous]
		[TestMethod]
		public void Can_insert_async_and_multi_get_async()
		{
			var documentStore = new DocumentStore { Url = url + port };
			documentStore.Initialize();

			var entity1 = new Company { Name = "Async Company #1" };
			var entity2 = new Company { Name = "Async Company #2" };
			using (var session_for_storing = documentStore.OpenAsyncSession())
			{
				session_for_storing.Store(entity1);
				session_for_storing.Store(entity2);
				EnqueueTaskCompleted(session_for_storing.SaveChangesAsync());
			}

			EnqueueCallback(() =>
			{
				using (var session_for_loading = documentStore.OpenAsyncSession())
				{
					var task = session_for_loading.MultiLoadAsync<Company>(new[] { entity1.Id, entity2.Id });
					EnqueueTaskCompleted(task);
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
			var documentStore = new DocumentStore { Url = url + port };
			documentStore.Initialize();

			var entity = new Company { Name = "Async Company #1" };
			using (var session_for_storing = documentStore.OpenAsyncSession())
			{
				session_for_storing.Store(entity);
				EnqueueTaskCompleted(session_for_storing.SaveChangesAsync());
			}

			EnqueueCallback(() =>
			{
				using (var session_for_loading = documentStore.OpenAsyncSession())
				{
					var task = session_for_loading.LoadAsync<Company>(entity.Id);
					EnqueueTaskCompleted(task);
					EnqueueCallback(() => Assert.Equal(entity.Name, task.Result.Name));
				}
				EnqueueTestComplete();
			});
		}

		[Asynchronous]
		[TestMethod]
		public void Can_insert_async_and_delete_async()
		{
			var documentStore = new DocumentStore { Url = url + port };
			documentStore.Initialize();

			var entity = new Company { Name = "Async Company #1", Id = "companies/1"};
			using (var session = documentStore.OpenAsyncSession())
			{
				session.Store(entity);
				EnqueueTaskCompleted(session.SaveChangesAsync());

				EnqueueCallback(()=>
				                	{
										using (var for_loading = documentStore.OpenAsyncSession())
										{
											var loading =for_loading.LoadAsync<Company>(entity.Id);
											EnqueueTaskCompleted(loading);
											EnqueueCallback(() => Assert.NotNull(loading.Result));
										}
				                	});

				EnqueueCallback(() =>
				{
				    using(var for_deleting = documentStore.OpenAsyncSession())
				    {
						var loading = for_deleting.LoadAsync<Company>(entity.Id);
						EnqueueTaskCompleted(loading);
						EnqueueCallback(() =>
						{
							for_deleting.Delete(loading.Result);
							EnqueueTaskCompleted(for_deleting.SaveChangesAsync());

							EnqueueCallback(() =>
					                	{
					                		using (var for_verifying = documentStore.OpenAsyncSession())
					                		{
					                			var verification = for_verifying.LoadAsync<Company>(entity.Id);
					                			EnqueueTaskCompleted(verification);
					                			EnqueueCallback(() => Assert.Null(verification.Result));
					                			EnqueueTestComplete();
					                		}
					                	});
						});
				    }
				});
			}
		}

		[Asynchronous]
		[TestMethod]
		public void Can_get_index_names_async()
		{
				var documentStore = new DocumentStore { Url = url + port };
				documentStore.Initialize();

				var task = documentStore.AsyncDatabaseCommands.GetIndexNamesAsync(0,25);
				EnqueueTaskCompleted(task);
				EnqueueCallback(()=> Assert.Equal(new[] { "Raven/DocumentsByEntityName" },task.Result));
				EnqueueTestComplete();
		}

		void EnqueueTaskCompleted(Task task)
		{
			EnqueueConditional(() => task.IsCompleted || task.IsFaulted);
		}
	}
}