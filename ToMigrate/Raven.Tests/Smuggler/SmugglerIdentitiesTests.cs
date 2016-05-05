// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2808.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Smuggler
{
    public class SmugglerIdentitiesTests : ReplicationBase
    {
        private class Foo
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string NameImport { get; set; }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task SmugglerTransformShouldWorkForDatabaseDataDumper()
        {
            var path = NewDataPath(forceCreateDir: true);
            var backupPath = Path.Combine(path, "backup.dump");

            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { Name = "N1" });
                    session.Store(new Foo { Name = "N2" });

                    session.SaveChanges();
                }
                var smugglerApi = new DatabaseDataDumper(store.DocumentDatabase);
                smugglerApi.Options.TransformScript =
                    @"function(doc) { 
                        var id = doc['@metadata']['@id']; 
                        if(id === 'foos/1')
                            return null;
                        return doc;
                    }";
                await smugglerApi.ExportData(
                    new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        ToFile = backupPath,
                        From = new EmbeddedRavenConnectionStringOptions
                        {
                            DefaultDatabase = store.DefaultDatabase
                        }
                    });
            }

            using (var documentStore = NewDocumentStore())
            {
                var smugglerApi = new DatabaseDataDumper(documentStore.DocumentDatabase);
                smugglerApi.Options.TransformScript =
                    @"function(doc) { 
                        var id = doc['@metadata']['@id']; 
                        if(id === 'foos/1')
                            return null;
                        return doc;
                    }";
                await smugglerApi.ImportData(
                    new SmugglerImportOptions<RavenConnectionStringOptions>
                    {
                        FromFile = backupPath,
                        To = new EmbeddedRavenConnectionStringOptions
                        {
                            DefaultDatabase = documentStore.DefaultDatabase
                        }
                    });

                using (var session = documentStore.OpenSession())
                {
                    var foos = session.Query<Foo>()
                                      .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                                      .ToList();

                    Assert.Equal(1, foos.Count);
                    Assert.Equal("foos/2", foos[0].Id);
                    Assert.Equal("N2", foos[0].Name);

                    Assert.Null(session.Load<Foo>(1));
                }
            }
        }

        [Fact]
        public void DuringRestoreDatabaseIdCanBeChanged()
        {
            var backupPath = NewDataPath();

            using (var store = NewRemoteDocumentStore(runInMemory: false))
            {
                store
                    .DatabaseCommands
                    .GlobalAdmin
                    .CreateDatabase(new DatabaseDocument
                    {
                        Id = "N1",
                        Settings =
                        {
                            { Constants.ActiveBundles, "Replication" },
                            { "Raven/DataDir", NewDataPath() }
                        }
                    });

                var commands = store.DatabaseCommands.ForDatabase("N1");
                var oldDatabaseId = store.DatabaseCommands.GetStatistics().DatabaseId;

                commands.GlobalAdmin.StartBackup(backupPath, null, incremental: false, databaseName: "N1").WaitForCompletion();

                var operation = commands
                    .GlobalAdmin
                    .StartRestore(new DatabaseRestoreRequest
                    {
                        BackupLocation = backupPath,
                        DatabaseName = "N3",
                        GenerateNewDatabaseId = true
                    });

                var status = operation.WaitForCompletion();

                var newDatabaseId = commands
                    .ForDatabase("N3")
                    .GetStatistics()
                    .DatabaseId;

                Assert.NotEqual(oldDatabaseId, newDatabaseId);
            }
        }

        [Fact]
        public void AfterRestoreDatabaseIdIsTheSame()
        {
            var backupPath = NewDataPath();

            using (var store = NewRemoteDocumentStore(runInMemory: false))
            {
                store
                    .DatabaseCommands
                    .GlobalAdmin
                    .CreateDatabase(new DatabaseDocument
                    {
                        Id = "N1",
                        Settings =
                        {
                            { Constants.ActiveBundles, "Replication" },
                            { "Raven/DataDir", NewDataPath() }
                        }
                    });

                var commands = store.DatabaseCommands.ForDatabase("N1");
                var oldDatabaseId = commands.GetStatistics().DatabaseId;

                commands.GlobalAdmin.StartBackup(backupPath, null, incremental: false, databaseName: "N1").WaitForCompletion();

                var operation = commands
                    .GlobalAdmin
                    .StartRestore(new DatabaseRestoreRequest
                    {
                        BackupLocation = backupPath,
                        DatabaseName = "N3",
                        GenerateNewDatabaseId = false
                    });

                var status = operation.WaitForCompletion();

                var newDatabaseId = commands
                    .ForDatabase("N3")
                    .GetStatistics()
                    .DatabaseId;

                Assert.Equal(oldDatabaseId, newDatabaseId);
            }
        }

        [Fact]
        public void SmugglerCanStripReplicationInformationDuringImport_Remote()
        {
            var path = NewDataPath(forceCreateDir: true);
            var backupPath = Path.Combine(path, "backup.dump");

            using (var store = NewRemoteDocumentStore(runInMemory: false))
            {
                store
                    .DatabaseCommands
                    .GlobalAdmin
                    .CreateDatabase(new DatabaseDocument
                    {
                        Id = "N1",
                        Settings =
                        {
                            { Constants.ActiveBundles, "Replication" },
                            { "Raven/DataDir", NewDataPath() }
                        }
                    });

                var commands = store.DatabaseCommands.ForDatabase("N1");
                commands.Put("keys/1", null, new RavenJObject(), new RavenJObject());
                var doc = commands.Get("keys/1");
                Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationSource));
                Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationVersion));

                commands.PutAttachment("keys/1", null, new MemoryStream(), new RavenJObject());
                var attachment = commands.GetAttachment("keys/1");
                Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationSource));
                Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationVersion));

                var smuggler = new SmugglerDatabaseApi(new SmugglerDatabaseOptions { StripReplicationInformation = true });
                smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = backupPath,
                    From = new RavenConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultDatabase = "N1"
                    }
                }).Wait(TimeSpan.FromSeconds(15));

                store
                    .DatabaseCommands
                    .GlobalAdmin
                    .CreateDatabase(new DatabaseDocument
                    {
                        Id = "N2",
                        Settings =
                        {
                            { Constants.ActiveBundles, "" },
                            { "Raven/DataDir", NewDataPath() }
                        }
                    });

                smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>
                {
                    FromFile = backupPath,
                    To = new RavenConnectionStringOptions
                    {
                        DefaultDatabase = "N2",
                        Url = store.Url
                    }
                }).Wait(TimeSpan.FromSeconds(15));

                commands = store.DatabaseCommands.ForDatabase("N2");
                doc = commands.Get("keys/1");
                Assert.False(doc.Metadata.ContainsKey(Constants.RavenReplicationSource));
                Assert.False(doc.Metadata.ContainsKey(Constants.RavenReplicationVersion));
                attachment = commands.GetAttachment("keys/1");
                Assert.False(attachment.Metadata.ContainsKey(Constants.RavenReplicationSource));
                Assert.False(attachment.Metadata.ContainsKey(Constants.RavenReplicationVersion));

                store
                    .DatabaseCommands
                    .GlobalAdmin
                    .CreateDatabase(new DatabaseDocument
                    {
                        Id = "N3",
                        Settings =
                        {
                            { Constants.ActiveBundles, "Replication" },
                            { "Raven/DataDir", NewDataPath() }
                        }
                    });

                smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>
                {
                    FromFile = backupPath,
                    To = new RavenConnectionStringOptions
                    {
                        DefaultDatabase = "N3",
                        Url = store.Url
                    }
                }).Wait(TimeSpan.FromSeconds(15));

                commands = store.DatabaseCommands.ForDatabase("N3");
                doc = commands.Get("keys/1");
                Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationSource));
                Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationVersion));
                attachment = commands.GetAttachment("keys/1");
                Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationSource));
                Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationVersion));
            }
        }

        [Fact]
        public void SmugglerCanStripReplicationInformationDuringImport_Embedded()
        {
            using (var stream = new MemoryStream())
            {
                using (var store = NewDocumentStore(activeBundles: "Replication"))
                {
                    var commands = store.DatabaseCommands;
                    commands.Put("keys/1", null, new RavenJObject(), new RavenJObject());
                    var doc = commands.Get("keys/1");
                    Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationSource));
                    Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationVersion));

                    commands.PutAttachment("keys/1", null, new MemoryStream(), new RavenJObject());
                    var attachment = commands.GetAttachment("keys/1");
                    Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationSource));
                    Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationVersion));

                    var smuggler = new DatabaseDataDumper(store.DocumentDatabase, new SmugglerDatabaseOptions { StripReplicationInformation = true });
                    smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToStream = stream, From = new RavenConnectionStringOptions { DefaultDatabase = store.DefaultDatabase } }).Wait(TimeSpan.FromSeconds(15));
                }

                stream.Position = 0;

                using (var store = NewDocumentStore())
                {
                    var smuggler = new DatabaseDataDumper(store.DocumentDatabase, new SmugglerDatabaseOptions { StripReplicationInformation = true });
                    smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>
                    {
                        FromStream = stream,
                        To = new RavenConnectionStringOptions
                        {
                            DefaultDatabase = store.DefaultDatabase,
                        }
                    }).Wait(TimeSpan.FromSeconds(15));

                    var commands = store.DatabaseCommands;
                    var doc = commands.Get("keys/1");
                    Assert.False(doc.Metadata.ContainsKey(Constants.RavenReplicationSource));
                    Assert.False(doc.Metadata.ContainsKey(Constants.RavenReplicationVersion));
                    var attachment = commands.GetAttachment("keys/1");
                    Assert.False(attachment.Metadata.ContainsKey(Constants.RavenReplicationSource));
                    Assert.False(attachment.Metadata.ContainsKey(Constants.RavenReplicationVersion));
                }

                stream.Position = 0;

                using (var store = NewDocumentStore(activeBundles: "Replication"))
                {
                    var smuggler = new DatabaseDataDumper(store.DocumentDatabase, new SmugglerDatabaseOptions { StripReplicationInformation = true });
                    smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>
                    {
                        FromStream = stream,
                        To = new RavenConnectionStringOptions
                        {
                            DefaultDatabase = store.DefaultDatabase,
                        }
                    }).Wait(TimeSpan.FromSeconds(15));

                    var commands = store.DatabaseCommands;
                    var doc = commands.Get("keys/1");
                    Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationSource));
                    Assert.True(doc.Metadata.ContainsKey(Constants.RavenReplicationVersion));
                    var attachment = commands.GetAttachment("keys/1");
                    Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationSource));
                    Assert.True(attachment.Metadata.ContainsKey(Constants.RavenReplicationVersion));
                }
            }

        }
    }
}
