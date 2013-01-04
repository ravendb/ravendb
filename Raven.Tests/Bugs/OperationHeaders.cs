//-----------------------------------------------------------------------
// <copyright file="OperationHeaders.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Reflection;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Document;
using Xunit;
using TransactionInformation = Raven.Abstractions.Data.TransactionInformation;

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
					((DocumentSession)session).DatabaseCommands.OperationsHeaders["Hello"] = "World";
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
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
				Port = 8079
			}))
			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"

			}.Initialize())
			{
				RecordOperationHeaders.Hello = null;
				using (var session = documentStore.OpenSession())
				{
					((DocumentSession)session).DatabaseCommands.OperationsHeaders["Hello"] = "World";
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
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
				Port = 8079
			}))
			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"

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

			public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
			{
				Hello = CurrentOperationContext.Headers.Value["Hello"];
				base.OnPut(key, document, metadata, transactionInformation);
			}
		}
	}
}
