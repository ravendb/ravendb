using System;
using System.IO;
using System.Threading;
using Raven.Client.Document;
using Raven.Server;
using Xunit;

namespace Raven.Client.Tests.Document
{
	public class AsyncDocumentStoreServerTests : BaseTest, IDisposable
	{
		private readonly string path;
		private readonly int port;

		public AsyncDocumentStoreServerTests()
		{
			port = 8080;
			path = GetPath("TestDb");
			RavenDbServer.EnsureCanListenToWhenInNonAdminContext(8080);
		}

		#region IDisposable Members

		public void Dispose()
		{
			Thread.Sleep(100);
			Directory.Delete(path, true);
		}

		#endregion

		[Fact]
		public void Can_insert_sync_and_get_async()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();

				var entity = new Company { Name = "Async Company" };
				using (var session = documentStore.OpenSession())
				{
					session.Store(entity);
					session.SaveChanges();
				}

				using (var session = documentStore.OpenAsyncSession())
				{
					var asyncResult = session.BeginLoad(entity.Id, null, null);
					if (asyncResult.CompletedSynchronously == false)
						asyncResult.AsyncWaitHandle.WaitOne();

					var company = session.EndLoad<Company>(asyncResult);
					Assert.Equal("Async Company", company.Name);
				}
			}
		}

		[Fact]
		public void Can_insert_async_and_get_sync()
		{
			using (var server = GetNewServer(port, path))
			{
				var documentStore = new DocumentStore { Url = "http://localhost:" + port };
				documentStore.Initialise();

				var entity = new Company { Name = "Async Company" };
				using (var session = documentStore.OpenAsyncSession())
				{
					session.Store(entity);
					var ar = session.BeginSaveChanges(null,null);
					ar.AsyncWaitHandle.WaitOne();
					session.EndSaveChanges(ar);
				}

				using (var session = documentStore.OpenSession())
				{
					var company = session.Load<Company>(entity.Id);

					Assert.Equal("Async Company", company.Name);
				}
			}
		}
	}
}