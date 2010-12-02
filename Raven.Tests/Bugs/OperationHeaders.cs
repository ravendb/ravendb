using System;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Reflection;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Http;
using Raven.Server;
using Raven.Tests.Document;
using Xunit;
using TransactionInformation = Raven.Http.TransactionInformation;

namespace Raven.Tests.Bugs
{
	public class OperationHeaders : IDisposable
	{
		private readonly string path;

		public OperationHeaders()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
		}

		public void Dispose()
		{
            IOExtensions.DeleteDirectory(path);
		}

		[Fact]
		public void CanPassOperationHeadersUsingEmbedded()
		{
            using (var documentStore = new EmbeddableDocumentStore
			{
				Configuration = 
				{
					Catalog =
						{
							Catalogs = { new TypeCatalog(typeof(RecordOperationHeaders)) }
						},
					DataDirectory = path,
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
				}

			}.Initialize())
			{
				RecordOperationHeaders.Hello = null;
				using(var session = documentStore.OpenSession())
				{
                    session.Advanced.DatabaseCommands.OperationsHeaders["Hello"] = "World";
					session.Store(new { Bar = "foo"});
					session.SaveChanges();

					Assert.Equal("World", RecordOperationHeaders.Hello);
				}
			}
		}

		[Fact]
		public void CanPassOperationHeadersUsingServer()
		{
			using (new RavenDbServer(new RavenConfiguration
			{
				Catalog =
				{
					Catalogs = { new TypeCatalog(typeof(RecordOperationHeaders)) }
				},
				DataDirectory = path,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
			}))
			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:8080"

			}.Initialize())
			{
				RecordOperationHeaders.Hello = null;
				using (var session = documentStore.OpenSession())
				{
                    session.Advanced.DatabaseCommands.OperationsHeaders["Hello"] = "World";
					session.Store(new { Bar = "foo" });
					session.SaveChanges();

					Assert.Equal("World", RecordOperationHeaders.Hello);
				}
			}
		}

		[Fact]
		public void CanPassOperationHeadersSetBeforeSessionUsingServer()
		{
			using (new RavenDbServer(new RavenConfiguration
			{
				Catalog =
				{
					Catalogs = { new TypeCatalog(typeof(RecordOperationHeaders)) }
				},
				DataDirectory = path,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
			}))
			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:8080"

			}.Initialize())
			{
				documentStore.SharedOperationsHeaders["Hello"] = "World";

				RecordOperationHeaders.Hello = null;
				using (var session = documentStore.OpenSession())
				{
					session.Store(new { Bar = "foo" });
					session.SaveChanges();

					Assert.Equal("World", RecordOperationHeaders.Hello);
				}
			}
		}

		public class RecordOperationHeaders : AbstractPutTrigger
		{
			public static string Hello;

			public override void OnPut(string key, Newtonsoft.Json.Linq.JObject document, Newtonsoft.Json.Linq.JObject metadata, TransactionInformation transactionInformation)
			{
				Hello = CurrentOperationContext.Headers.Value["Hello"];
				base.OnPut(key, document, metadata, transactionInformation);
			}
		}
	}
}
