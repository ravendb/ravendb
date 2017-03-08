//-----------------------------------------------------------------------
// <copyright file="HiLoServerKeysNotExported.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Lucene.Net.Support;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Smuggler;
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

            server = GetNewServer();
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

            var smugglerApi = new SmugglerDatabaseApi { Options = { Incremental = false } };
            var connection = new RavenConnectionStringOptions {Url = "http://localhost:8079/"};
            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    From = connection,
                    ToFile = DumpFile
                }).Wait(TimeSpan.FromSeconds(15));
            Assert.True(File.Exists(DumpFile));

            using (var session = documentStore.OpenSession())
            {
                var hilo = session.Load<HiLoKey>("Raven/Hilo/foos");
                Assert.NotNull(hilo);
                Assert.Equal(32, hilo.Max);
            }

            server.Dispose();
            CreateServer();

            smugglerApi.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = DumpFile, To = connection }).Wait(TimeSpan.FromSeconds(15));

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

            var connection = new RavenConnectionStringOptions { Url = "http://localhost:8079/" };
            var smugglerApi = new SmugglerDatabaseApi();
            smugglerApi.Options.Filters.Add(
                new FilterSetting
                {
                    Path = "Something",
                    ShouldMatch = true,
                    Values = new EquatableList<string> { "Something1" }
                });
            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = DumpFile,
                    From = connection,
                }).Wait(TimeSpan.FromSeconds(15));
            Assert.True(File.Exists(DumpFile));

            server.Dispose();
            CreateServer();

            smugglerApi.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = DumpFile, To = connection }).Wait(TimeSpan.FromSeconds(15));

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

            var connection = new RavenConnectionStringOptions { Url = "http://localhost:8079/" };
            var smugglerApi = new SmugglerDatabaseApi { Options = { OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments } };
            smugglerApi.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = DumpFile, From = connection }).Wait(TimeSpan.FromSeconds(15));

            Assert.True(File.Exists(DumpFile));

            server.Dispose();
            CreateServer();

            smugglerApi.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = DumpFile, To = connection }).Wait(TimeSpan.FromSeconds(15));

            var attachment = documentStore.DatabaseCommands.GetAttachment("test");
            Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data().ReadData());
            Assert.True(attachment.Metadata.Value<bool>("Test"));
        }

        [Fact]
        public void Export_And_Import_Incremental_Attachments()
        {
            var file = Path.Combine(NewDataPath(), "Incremental");
            IOExtensions.DeleteDirectory(file);

            documentStore.DatabaseCommands.PutAttachment("test", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject { { "Test", true } });

            var connection = new RavenConnectionStringOptions { Url = "http://localhost:8079/" };
            var smugglerApi = new SmugglerDatabaseApi { Options = { OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments, Incremental = true } };
            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = file,
                    From = connection,
                }).Wait(TimeSpan.FromSeconds(15));

            documentStore.DatabaseCommands.PutAttachment("test2", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject { { "Test2", true } });

            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = file,
                    From = connection,
                }).Wait(TimeSpan.FromSeconds(15));

            server.Dispose();
            CreateServer();

            smugglerApi.Options.Incremental = true;
            smugglerApi.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = file, To = connection }).Wait(TimeSpan.FromSeconds(15));

            var attachment = documentStore.DatabaseCommands.GetAttachment("test");
            Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data().ReadData());
            Assert.True(attachment.Metadata.Value<bool>("Test"));

            attachment = documentStore.DatabaseCommands.GetAttachment("test2");
            Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data().ReadData());
            Assert.True(attachment.Metadata.Value<bool>("Test2"));
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

            var connection = new RavenConnectionStringOptions { Url = "http://localhost:8079/" };
            var smugglerApi = new SmugglerDatabaseApi { Options = { OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments, Incremental = true } };
            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = file, From = connection,
                }).Wait(TimeSpan.FromSeconds(15));

            documentStore.DatabaseCommands.PutIndex("Index2", new IndexDefinition
            {
                Map = "from x in docs select new { x.Title, Count = 1}",
            });

            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = file,
                    From = connection,
                }).Wait(TimeSpan.FromSeconds(15));

            server.Dispose();
            CreateServer();

            smugglerApi.Options.Incremental = true;
            smugglerApi.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = file, To = connection }).Wait(TimeSpan.FromSeconds(15));

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

            var connection = new RavenConnectionStringOptions { Url = "http://localhost:8079/" };
            var smugglerApi = new SmugglerDatabaseApi { Options = { OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments, Incremental = true } };
            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = file,
                    From = connection,
                }).Wait(TimeSpan.FromSeconds(15));

            documentStore.DatabaseCommands.DeleteIndex("Index1");

            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = file,
                    From = connection,
                }).Wait(TimeSpan.FromSeconds(15));

            server.Dispose();
            CreateServer();

            smugglerApi.Options.Incremental = true;
            smugglerApi.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = file, To = connection }).Wait(TimeSpan.FromSeconds(15));

            var index = documentStore.DatabaseCommands.GetIndex("Index1");
            Assert.Null(index);
        }

        [Fact]
        public void Export_Incremental_not_overwrites_Files()
        {
            var file = Path.Combine(NewDataPath(), "Incremental");
            IOExtensions.DeleteDirectory(file);

            var connection = new RavenConnectionStringOptions { Url = "http://localhost:8079/" };
            var smugglerApi = new SmugglerDatabaseApi();
            smugglerApi.Options.OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments;
            smugglerApi.Options.Incremental = true;
            for (int i = 0; i < 50; i++)
            {
                smugglerApi.ExportData(
                    new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        ToFile = file,
                        From = connection,
                    }).Wait(TimeSpan.FromSeconds(15));
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

            var connection = new RavenConnectionStringOptions { Url = "http://localhost:8079/" };
            var smugglerApi = new SmugglerDatabaseApi { Options = { OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments, Incremental = true } };
            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = file,
                    From = connection,
                }).Wait(TimeSpan.FromSeconds(15));

            using (var session = documentStore.OpenSession())
            {
                var doc = session.Load<Foo>("Test/1");
                doc.Something = "After Change";
                session.SaveChanges();
            }

            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = file,
                    From = connection,
                }).Wait(TimeSpan.FromSeconds(15));

            server.Dispose();
            CreateServer();

            smugglerApi.Options.Incremental = true;
            smugglerApi.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = file, To = connection }).Wait(TimeSpan.FromSeconds(15));

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

            var connection = new RavenConnectionStringOptions { Url = "http://localhost:8079/" };
            var smugglerApi = new SmugglerDatabaseApi { Options = { OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments, Incremental = true } };
            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = file,
                    From = connection,
                }).Wait(TimeSpan.FromSeconds(15));

            using (var session = documentStore.OpenSession())
            {
                var foo = new Foo { Something = "Something2", Id = "Test/2" };
                session.Store(foo);
                session.SaveChanges();
            }

            smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = file,
                    From = connection,
                }).Wait(TimeSpan.FromSeconds(15));

            server.Dispose();
            CreateServer();

            smugglerApi.Options.Incremental = true;
            smugglerApi.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = file, To = connection }).Wait(TimeSpan.FromSeconds(15));

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
