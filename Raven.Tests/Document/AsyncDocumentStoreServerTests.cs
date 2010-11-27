using System;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Http;
using Xunit;

namespace Raven.Tests.Document
{
	public class AsyncDocumentStoreServerTests : RemoteClientTest, IDisposable
	{
		private readonly string path;
		private readonly int port;

		public AsyncDocumentStoreServerTests()
		{
			port = 8080;
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
		}

		#region IDisposable Members

		public void Dispose()
		{
            IOExtensions.DeleteDirectory(path);
		}

		#endregion

        [Fact]
        public void Can_insert_sync_and_get_async()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				var entity = new Company { Name = "Async Company" };
				using (var session = documentStore.OpenSession())
				{
					session.Store(entity);
					session.SaveChanges();
				}

				using (var session = documentStore.OpenAsyncSession())
				{
					var task = session.LoadAsync<Company>(entity.Id);

					Assert.Equal("Async Company", task.Result.Name);
				}
			}
		}

        [Fact]
        public void Can_insert_async_and_get_sync()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				var entity = new Company { Name = "Async Company" };
				using (var session = documentStore.OpenAsyncSession())
				{
					session.Store(entity);
					session.SaveChangesAsync().Wait();
				}

				using (var session = documentStore.OpenSession())
				{
					var company = session.Load<Company>(entity.Id);

					Assert.Equal("Async Company", company.Name);
				}
			}
		}

        [Fact]
		public void Can_insert_async_and_multi_get_async()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialize();

				var entity1 = new Company { Name = "Async Company #1" };
				var entity2 = new Company { Name = "Async Company #2" };
				using (var session = documentStore.OpenAsyncSession())
				{
					session.Store(entity1);
					session.Store(entity2);
					session.SaveChangesAsync().Wait();
				}

				using (var session = documentStore.OpenAsyncSession())
				{
					var task = session.MultiLoadAsync<Company>(new[]{entity1.Id, entity2.Id});
					Assert.Equal(entity1.Name, task.Result[0].Name);
					Assert.Equal(entity2.Name, task.Result[1].Name);
				}
			}
		}
	}
}
