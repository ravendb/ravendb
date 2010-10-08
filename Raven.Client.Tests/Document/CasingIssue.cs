using System;
using System.IO;
using System.Reflection;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Extensions;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Document
{
	public class CasingIssue : RemoteClientTest, IDisposable
	{
		private string path;

		#region IDisposable Members

		public void Dispose()
		{
            IOExtensions.DeleteDirectory(path);
		}

		#endregion


		private DocumentStore NewDocumentStore()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			var documentStore = new DocumentStore
			{
				Configuration = new RavenConfiguration
				{
					DataDirectory = path,
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
				}
				
			};
			documentStore.Initialize();
			return documentStore;
		}

		[Fact]
		public void CanQueryByEntityType()
		{
			using(var store = NewDocumentStore())
			using(var session = store.OpenSession())
			{
				session.Store(new Post{Title = "test", Body = "casing"});
				session.SaveChanges();

				var single = session.LuceneQuery<Post>()
					.WaitForNonStaleResults()
					.Single();

				Assert.Equal("test", single.Title);
			}
		}

		[Fact]
		public void UnitOfWorkEvenWhenQuerying()
		{
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				var entity = new Post { Title = "test", Body = "casing" };
				session.Store(entity);
				session.SaveChanges();

				var single = session.LuceneQuery<Post>()
					.WaitForNonStaleResults()
					.Single();

				Assert.Same(entity, single);
			}
		}

		public class Post
		{
			public string Id { get; set; }
			public string Title { get; set; }
			public string Body { get; set; }
		}

	}
}