extern alias database;
using System;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Bundles.CascadeDelete;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;
using System.Linq;

namespace Raven.Bundles.Tests.Expiration
{
	public class WithCascade : IDisposable
	{
		private readonly DocumentStore documentStore;
		private readonly string path;
		private readonly RavenDbServer ravenDbServer;

		public WithCascade()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(WithCascade)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory("Data");
			var ravenConfiguration = new database::Raven.Database.Config.RavenConfiguration
			{
				Port = 8079,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
				DataDirectory = path,
				Catalog =
					{
						Catalogs =
							{
								new AssemblyCatalog(typeof(CascadeDeleteTrigger).Assembly)
							}
					},
				Settings =
					{
						{"Raven/Expiration/DeleteFrequencySeconds", "1"},
						{"Raven/ActiveBundles", "documentExpiration"}
			}
			};
			ravenConfiguration.PostInit();
			ravenDbServer = new RavenDbServer(ravenConfiguration);
			documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			};
			documentStore.Initialize();
		}


		[Fact]
		public void CanDeleteAndCascadeAtTheSameTime()
		{
			documentStore.DatabaseCommands.PutAttachment("item", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
			using (var session = documentStore.OpenSession())
			{
				var doc = new { Id = "doc/1" };
				session.Store(doc);
				session.Advanced.GetMetadataFor(doc)["Raven-Expiration-Date"] = DateTime.Now.AddDays(-15);
				session.Advanced.GetMetadataFor(doc)[MetadataKeys.AttachmentsToCascadeDelete] = new RavenJArray(new[] { "item" });
				session.SaveChanges();
			}
			
			JsonDocument documentByKey = null;
			for (int i = 0; i < 50; i++)
			{
				ravenDbServer.SystemDatabase.TransactionalStorage.Batch(accessor =>
				{
					documentByKey = accessor.Documents.DocumentByKey("doc/1", null);

				});
				if (documentByKey == null)
					break;
				Thread.Sleep(100);
			}

			Assert.Null(documentByKey);


			ravenDbServer.SystemDatabase.TransactionalStorage.Batch(accessor => Assert.Null(accessor.Attachments.GetAttachment("item")));
		
		}

		[Fact]
		public void CanDeleteAndCascadeAtTheSameTimeDocuemnts()
		{
		//	documentStore.DatabaseCommands.Put("doc/1", new Etag(), new RavenJObject(), new RavenJObject());
			using (var session = documentStore.OpenSession())
			{
				var doc1 = new { Id = "doc/1" };
				var doc2 = new { Id = "doc/2" };
				session.Store(doc1);
				session.Store(doc2);
				session.Advanced.GetMetadataFor(doc1)["Raven-Expiration-Date"] = DateTime.Now.AddDays(-15);
				session.Advanced.GetMetadataFor(doc1)[MetadataKeys.DocumentsToCascadeDelete] = new RavenJArray(new[] { "doc/2" });
				session.SaveChanges();
			}

			JsonDocument documentByKey = null;
			for (int i = 0; i < 50; i++)
			{
				ravenDbServer.SystemDatabase.TransactionalStorage.Batch(accessor =>
				{
					documentByKey = accessor.Documents.DocumentByKey("doc/1", null);

				});
				if (documentByKey == null)
					break;
				Thread.Sleep(100);
			}

			Assert.Null(documentByKey);


			ravenDbServer.SystemDatabase.TransactionalStorage.Batch(accessor => Assert.Null(accessor.Documents.DocumentByKey("doc/2", null)));
		}

		[Fact]
		public void CanDeleteMultiChildrenWithCascade()
		{
			using (var session = documentStore.OpenSession())
			{
				var parent = new Foo();
				var child1 = new Foo();
				var child2 = new Foo();
				session.Store(parent, "parentId1");
				session.Store(child1, "childId1");
				session.Store(child2, "childId2");
				session.Advanced.GetMetadataFor(parent)["Raven-Cascade-Delete-Documents"] = RavenJToken.FromObject(new[] { "childId1", "childId2" });
				session.Advanced.GetMetadataFor(parent)["Raven-Expiration-Date"] = new RavenJValue(DateTime.UtcNow.AddSeconds(4));
				session.SaveChanges();
			}

			Thread.Sleep(5000);
			using (var session = documentStore.OpenSession())
			{
				var list = session.Query<Foo>().ToList();
				Assert.Empty(list);
			}
		}

		public class Foo
		{

		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		/// <filterpriority>2</filterpriority>
		public void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
		}
	}
}