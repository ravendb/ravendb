//-----------------------------------------------------------------------
// <copyright file="HiLoServerKeysNotExported.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class HiLoServerKeysNotExported : IDisposable
	{
		private DocumentStore documentStore;
		private RavenDbServer server;

		public HiLoServerKeysNotExported()
		{
			CreateServer(true);


		}

		private void CreateServer(bool initDocStore = false)
		{
			IOExtensions.DeleteDirectory("HiLoData");
			server = new RavenDbServer(new RavenConfiguration
			{
				Port = 8079, 
				DataDirectory = "HiLoData", 
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
				AnonymousUserAccessMode = AnonymousUserAccessMode.All
			});

			if (initDocStore) {
				documentStore = new DocumentStore() { Url = "http://localhost:8079/" };
				documentStore.Initialize();
			}
		}

		[Fact]
		public void Export_And_Import_Retains_HiLoState()
		{
			using (var session = documentStore.OpenSession()) {
				var foo = new Foo() { Something = "something2" };
				Assert.Null(foo.Id);
				session.Store(foo);
				Assert.NotNull(foo.Id);
				session.SaveChanges();
			}

			if (File.Exists("hilo-export.dump"))
				File.Delete("hilo-export.dump");
			Smuggler.Smuggler.ExportData(new Smuggler.Smuggler.SmugglerOptions
			{
				InstanceUrl = "http://localhost:8079/",
				File = "hilo-export.dump",
			});
			Assert.True(File.Exists("hilo-export.dump"));

			using (var session = documentStore.OpenSession()) {
				var hilo = session.Load<HiLoKey>("Raven/Hilo/foos");
				Assert.NotNull(hilo);
				Assert.Equal(32, hilo.Max);
			}

			server.Dispose();
			CreateServer();

			Smuggler.Smuggler.ImportData(new Smuggler.Smuggler.SmugglerOptions
			{
				File = "hilo-export.dump",
				InstanceUrl = "http://localhost:8079/",
			});

			using (var session = documentStore.OpenSession()) {
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
				session.Store(new Foo { Something = "something1" });
				session.Store(new Foo { Something = "something2" });
				session.SaveChanges();
			}

			if (File.Exists("hilo-export.dump"))
				File.Delete("hilo-export.dump");
			Smuggler.Smuggler.ExportData(new Smuggler.Smuggler.SmugglerOptions
			{
				InstanceUrl = "http://localhost:8079/",
				File = "hilo-export.dump",
				Filters =
			                             	{
			                             		{"Something", "something1"}
			                             	}
			});
			Assert.True(File.Exists("hilo-export.dump"));

			server.Dispose();
			CreateServer();

			Smuggler.Smuggler.ImportData(new Smuggler.Smuggler.SmugglerOptions
			{
				File = "hilo-export.dump",
				InstanceUrl = "http://localhost:8079/",
			});

			using (var session = documentStore.OpenSession())
			{
				Assert.NotNull(session.Load<Foo>("foos/1"));
				Assert.Null(session.Load<Foo>("foos/2"));
			}
		}

		[Fact]
		public void Export_And_Import_Retains_Attachment_Metadata()
		{
			documentStore.DatabaseCommands.PutAttachment("test", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject { { "Test", true } });

			if (File.Exists("hilo-export.dump"))
				File.Delete("hilo-export.dump");
			Smuggler.Smuggler.ExportData(new Smuggler.Smuggler.SmugglerOptions
			{
				InstanceUrl = "http://localhost:8079/", 
				File = "hilo-export.dump", 
				IncludeAttachments = true, 
				ExportIndexesOnly = false
			});
			Assert.True(File.Exists("hilo-export.dump"));

			server.Dispose();
			CreateServer();

			Smuggler.Smuggler.ImportData(new Smuggler.Smuggler.SmugglerOptions
			{
				InstanceUrl = "http://localhost:8079/",
				File = "hilo-export.dump"
			});

			var attachment = documentStore.DatabaseCommands.GetAttachment("test");
			Assert.Equal(new byte[]{1,2,3}, attachment.Data().ReadData());
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
