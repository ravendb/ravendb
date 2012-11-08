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
						{"Raven/ActiveBundles", "Expiration"}
			}
			};
			ravenConfiguration.PostInit();
			ravenDbServer = new RavenDbServer(ravenConfiguration);
			database::Raven.Bundles.Expiration.ExpirationReadTrigger.GetCurrentUtcDate = () => DateTime.UtcNow;
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
				ravenDbServer.Database.TransactionalStorage.Batch(accessor =>
				{
					documentByKey = accessor.Documents.DocumentByKey("doc/1", null);

				});
				if (documentByKey == null)
					break;
				Thread.Sleep(100);
			}

			Assert.Null(documentByKey);


			ravenDbServer.Database.TransactionalStorage.Batch(accessor => Assert.Null(accessor.Attachments.GetAttachment("item")));
		
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