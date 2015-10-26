//-----------------------------------------------------------------------
// <copyright file="HiLoServerKeysNotExported.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Lucene.Net.Support;

using Raven.Abstractions.Database.Smuggler.Common;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Server;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Files;
using Raven.Smuggler.Database.Remote;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class HiLoServerKeysNotExported : RavenTest
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
			if(server != null)
				server.Dispose();

			server = new RavenDbServer(new RavenConfiguration
										{
											Port = 8079,
											DataDirectory = "HiLoData",
											RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
											AnonymousUserAccessMode = AnonymousUserAccessMode.Admin
										})
			{
				UseEmbeddedHttpServer = true
			};
			server.Initialize();

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

		    var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions { Url = documentStore.Url };

		    var smuggler = new DatabaseSmuggler(
                new DatabaseSmugglerOptions(),
                new DatabaseSmugglerRemoteSource(connectionOptions),
                new DatabaseSmugglerFileDestination(DumpFile));

		    smuggler.Execute();

			Assert.True(File.Exists(DumpFile));

			using (var session = documentStore.OpenSession())
			{
				var hilo = session.Load<HiLoKey>("Raven/Hilo/foos");
				Assert.NotNull(hilo);
				Assert.Equal(32, hilo.Max);
			}

			server.Dispose();
			CreateServer();

            smuggler = new DatabaseSmuggler(
                new DatabaseSmugglerOptions(),
                new DatabaseSmugglerFileSource(DumpFile), 
                new DatabaseSmugglerRemoteDestination(connectionOptions));

            smuggler.Execute();

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
				session.Store(new Foo { Something = "something1" });
				session.Store(new Foo { Something = "something2" });
				session.SaveChanges();
			}

            var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions { Url = documentStore.Url };

		    var options = new DatabaseSmugglerOptions();
            options.Filters.Add(
                new FilterSetting
                {
                    Path = "Something",
                    ShouldMatch = true,
                    Values = new EquatableList<string> { "Something1" }
                });

            var smuggler = new DatabaseSmuggler(
                options,
                new DatabaseSmugglerRemoteSource(connectionOptions),
                new DatabaseSmugglerFileDestination(DumpFile));

            smuggler.Execute();

			Assert.True(File.Exists(DumpFile));

			server.Dispose();
			CreateServer();

            smuggler = new DatabaseSmuggler(
                options,
                new DatabaseSmugglerFileSource(DumpFile), 
                new DatabaseSmugglerRemoteDestination(connectionOptions));

            smuggler.Execute();

			using (var session = documentStore.OpenSession())
			{
				Assert.NotNull(session.Load<Foo>("foos/1"));
				Assert.Null(session.Load<Foo>("foos/2"));
			}
		}

		[Fact]
		public void Export_And_Import_Incremental_Indexes()
		{
			var file = Path.Combine(NewDataPath(), "Incremental");
			IOExtensions.DeleteDirectory(file);

			documentStore.DatabaseCommands.PutIndex("Index1", new IndexDefinition
			{
				Map = "from x in docs select new { x.Name, Count = 1}",
			});

            var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions { Url = documentStore.Url };
            var options = new DatabaseSmugglerOptions();
            options.OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Indexes;

            var smuggler = new DatabaseSmuggler(
                options,
                new DatabaseSmugglerRemoteSource(connectionOptions),
                new DatabaseSmugglerFileDestination(file, new DatabaseSmugglerFileDestinationOptions
                {
                    Incremental = true
                }));

            smuggler.Execute();

			documentStore.DatabaseCommands.PutIndex("Index2", new IndexDefinition
			{
				Map = "from x in docs select new { x.Title, Count = 1}",
			});

            smuggler.Execute();

			server.Dispose();
			CreateServer();

            smuggler = new DatabaseSmuggler(
                options, 
                new DatabaseSmugglerFileSource(file),
                new DatabaseSmugglerRemoteDestination(connectionOptions));

		    smuggler.Execute();

			var index = documentStore.DatabaseCommands.GetIndex("Index1");
			Assert.NotNull(index);

			index = documentStore.DatabaseCommands.GetIndex("Index2");
			Assert.NotNull(index);
		}

		[Fact]
		public void Export_And_Import_Incremental_Indexes_delete()
		{
			var file = Path.Combine(NewDataPath(), "Incremental");
			IOExtensions.DeleteDirectory(file);

			documentStore.DatabaseCommands.PutIndex("Index1", new IndexDefinition
			{
				Map = "from x in docs select new { x.Name, Count = 1}",
			});

            var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions { Url = documentStore.Url };
            var options = new DatabaseSmugglerOptions();
            options.OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Indexes;

            var smuggler = new DatabaseSmuggler(
                options,
                new DatabaseSmugglerRemoteSource(connectionOptions),
                new DatabaseSmugglerFileDestination(file, new DatabaseSmugglerFileDestinationOptions
                {
                    Incremental = true
                }));

            documentStore.DatabaseCommands.DeleteIndex("Index1");

		    smuggler.Execute();

			server.Dispose();
			CreateServer();

            smuggler = new DatabaseSmuggler(
                options,
                new DatabaseSmugglerFileSource(file),
                new DatabaseSmugglerRemoteDestination(connectionOptions));

            smuggler.Execute();

            var index = documentStore.DatabaseCommands.GetIndex("Index1");
			Assert.Null(index);
		}

		[Fact]
		public void Export_Incremental_not_overwrites_Files()
		{
			var file = Path.Combine(NewDataPath(), "Incremental");
			IOExtensions.DeleteDirectory(file);

            var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions { Url = documentStore.Url };
            var options = new DatabaseSmugglerOptions();
            options.OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Indexes;

            var smuggler = new DatabaseSmuggler(
                options,
                new DatabaseSmugglerRemoteSource(connectionOptions),
                new DatabaseSmugglerFileDestination(file, new DatabaseSmugglerFileDestinationOptions
                {
                    Incremental = true
                }));

            for (int i = 0; i < 50; i++)
            {
                smuggler.Execute();
            }

			Assert.Equal(Directory.GetFiles(file).Length, 51);//50 .dump.inc files and 1 LastEtags.txt
		}

		[Fact]
		public void Export_And_Import_Incremental_Changed_Document()
		{
			var file = Path.Combine(NewDataPath(), "Incremental");
			IOExtensions.DeleteDirectory(file);

			using (var session = documentStore.OpenSession())
			{
				var foo = new Foo {Something = "Before Change", Id = "Test/1"};
				session.Store(foo);
				session.SaveChanges();
			}

            var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions { Url = documentStore.Url };
            var options = new DatabaseSmugglerOptions();
            options.OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Indexes;

            var smuggler = new DatabaseSmuggler(
                options,
                new DatabaseSmugglerRemoteSource(connectionOptions),
                new DatabaseSmugglerFileDestination(file, new DatabaseSmugglerFileDestinationOptions
                {
                    Incremental = true
                }));

		    smuggler.Execute();

			using (var session = documentStore.OpenSession())
			{
				var doc = session.Load<Foo>("Test/1");
				doc.Something = "After Change";
				session.SaveChanges();
			}

            smuggler.Execute();

            server.Dispose();
			CreateServer();

            smuggler = new DatabaseSmuggler(
                options,
                new DatabaseSmugglerFileSource(file),
                new DatabaseSmugglerRemoteDestination(connectionOptions));

            smuggler.Execute();

			using (var session = documentStore.OpenSession())
			{
				var doc = session.Load<Foo>("Test/1");
				Assert.Equal(doc.Something, "After Change");
			}
		}

		[Fact]
		public void Export_And_Import_Incremental_Documents()
		{
			var file = Path.Combine(NewDataPath(), "Incremental");
			IOExtensions.DeleteDirectory(file);

			using (var session = documentStore.OpenSession())
			{
				var foo = new Foo { Something = "Something1", Id = "Test/1" };
				session.Store(foo);
				session.SaveChanges();
			}

            var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions { Url = documentStore.Url };
            var options = new DatabaseSmugglerOptions();
            options.OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Indexes;

            var smuggler = new DatabaseSmuggler(
                options,
                new DatabaseSmugglerRemoteSource(connectionOptions),
                new DatabaseSmugglerFileDestination(file, new DatabaseSmugglerFileDestinationOptions
                {
                    Incremental = true
                }));

            smuggler.Execute();

            using (var session = documentStore.OpenSession())
			{
				var foo = new Foo { Something = "Something2", Id = "Test/2" };
				session.Store(foo);
				session.SaveChanges();
			}

		    smuggler.Execute();

			server.Dispose();
			CreateServer();

            smuggler = new DatabaseSmuggler(
                options,
                new DatabaseSmugglerFileSource(file),
                new DatabaseSmugglerRemoteDestination(connectionOptions));

            smuggler.Execute();

            using (var session = documentStore.OpenSession())
			{
				var doc = session.Load<Foo>("Test/1");
				Assert.Equal(doc.Something, "Something1");
				doc = session.Load<Foo>("Test/2");
				Assert.Equal(doc.Something, "Something2");
			}
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

		public override void Dispose()
		{
			documentStore.Dispose();
			server.Dispose();
			IOExtensions.DeleteDirectory("HiLoData");
			base.Dispose();
		}
	}
}
