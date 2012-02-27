//-----------------------------------------------------------------------
// <copyright file="HiLoServerKeysNotExported.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Smuggler;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class HiLoServerKeysNotExported : RavenTest, IDisposable
	{
		private const string DumpFile = "hilo-export.dump";

		private readonly DocumentStore documentStore;
		private RavenDbServer server;

		public HiLoServerKeysNotExported()
		{
			CreateServer();

			documentStore = new DocumentStore { Url = "http://localhost:8079/" };
			documentStore.Initialize();

			if (File.Exists(DumpFile))
				File.Delete(DumpFile);
		}

		private void CreateServer()
		{
			IOExtensions.DeleteDirectory("HiLoData");
			server = new RavenDbServer(new RavenConfiguration
			                           	{
			                           		Port = 8079,
			                           		DataDirectory = "HiLoData",
			                           		RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
			                           		AnonymousUserAccessMode = AnonymousUserAccessMode.All
			                           	});

		}

		[Fact]
		public void Export_And_Import_Retains_HiLoState()
		{
			using (var session = documentStore.OpenSession())
			{
				var foo = new Foo {Something = "something2"};
				Assert.Null(foo.Id);
				session.Store(foo);
				Assert.NotNull(foo.Id);
				session.SaveChanges();
			}

			var smugglerApi = new SmugglerApi(new RavenConnectionStringOptions {Url = "http://localhost:8079/"});
			smugglerApi.ExportData(new SmugglerOptions {File = DumpFile});
			Assert.True(File.Exists(DumpFile));

			using (var session = documentStore.OpenSession())
			{
				var hilo = session.Load<HiLoKey>("Raven/Hilo/foos");
				Assert.NotNull(hilo);
				Assert.Equal(32, hilo.Max);
			}

			server.Dispose();
			CreateServer();

			smugglerApi.ImportData(new SmugglerOptions {File = DumpFile});

			using (var session = documentStore.OpenSession())
			{
				var hilo = session.Load<HiLoKey>("Raven/Hilo/foos");
				Assert.NotNull(hilo);
				Assert.Equal(32, hilo.Max);
			}
		}

		[Fact]
		public void Can_filter_documents()
		{
			using (var session = documentStore.OpenSession())
			{
				session.Store(new Foo {Something = "something1"});
				session.Store(new Foo {Something = "something2"});
				session.SaveChanges();
			}

			var smugglerApi = new SmugglerApi(new RavenConnectionStringOptions {Url = "http://localhost:8079/"});
			smugglerApi.ExportData(new SmugglerOptions
			                       	{
			                       		File = DumpFile,
			                       		Filters =
			                       			{
			                       				{"Something", "something1"}
			                       			}
			                       	});
			Assert.True(File.Exists(DumpFile));

			server.Dispose();
			CreateServer();

			smugglerApi.ImportData(new SmugglerOptions {File = DumpFile});
			
			using (var session = documentStore.OpenSession())
			{
				Assert.NotNull(session.Load<Foo>("foos/1"));
				Assert.Null(session.Load<Foo>("foos/2"));
			}
		}

		[Fact]
		public void Export_And_Import_Retains_Attachment_Metadata()
		{
			documentStore.DatabaseCommands.PutAttachment("test", null, new MemoryStream(new byte[] {1, 2, 3}), new RavenJObject {{"Test", true}});

			var smugglerApi = new SmugglerApi(new RavenConnectionStringOptions {Url = "http://localhost:8079/"});
			smugglerApi.ExportData(new SmugglerOptions
			                       	{
			                       		File = DumpFile,
			                       		OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments
			                       	});

			Assert.True(File.Exists(DumpFile));

			server.Dispose();
			CreateServer();

			smugglerApi.ImportData(new SmugglerOptions {File = DumpFile});

			var attachment = documentStore.DatabaseCommands.GetAttachment("test");
			Assert.Equal(new byte[] {1, 2, 3}, attachment.Data().ReadData());
			Assert.True(attachment.Metadata.Value<bool>("Test"));
		}

		public class Foo
		{
			public string Id { get; set; }
			public string Something { get; set; }
		}

		private class HiLoKey
		{
			public long Max { get; set; }

		}

		public void Dispose()
		{
			documentStore.Dispose();
			server.Dispose();
			IOExtensions.DeleteDirectory("HiLoData");
		}
	}
}