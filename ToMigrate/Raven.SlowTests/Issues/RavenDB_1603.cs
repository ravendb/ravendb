// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1603.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Common;
using Raven.Tests.Common.Triggers;
using Raven.Tests.Common.Util;

using Xunit;

namespace Raven.SlowTests.Issues
{


    public class RavenDB_1603 : RavenTest
    {
        public class User
        {
            public string Name { get; set; }
            public string Id { get; set; }
        }

        public class Developer
        {
            public string Name { get; set; }
            public string Id { get; set; }
        }
        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.Container = new CompositionContainer(new TypeCatalog(
                                                               typeof(HiddenDocumentsTrigger)));
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanPerformDump_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    InsertUsers(store, 0, 2000);

                    var dumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Incremental = true } };
                    await dumper.ExportData(
                        new SmugglerExportOptions<RavenConnectionStringOptions>
                        {
                            ToFile = backupPath,
                        });
                }

                VerifyDump(backupPath, store =>
                {
                    using (var session = store.OpenSession())
                    {
                        Assert.Equal(2000, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                    }
                });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanPerformDump_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    InsertUsers(store, 0, 2000);

                    var dumper = new SmugglerDatabaseApi { Options = { Incremental = true } };
                    await dumper.ExportData(
                        new SmugglerExportOptions<RavenConnectionStringOptions>
                        {
                            ToFile = backupPath,
                            From = new RavenConnectionStringOptions
                            {
                                Url = "http://localhost:8079",
                                DefaultDatabase = store.DefaultDatabase,
                            }
                        });
                }

                VerifyDump(backupPath, store =>
                {
                    using (var session = store.OpenSession())
                    {
                        Assert.Equal(2000, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                    }
                });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanPerformDumpWithLimit_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    InsertUsers(store, 0, 2000);

                    var dumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Limit = 1500, Incremental = true } };
                    dumper.Options.Filters.Add(
                        new FilterSetting
                        {
                            Path = "@metadata.Raven-Entity-Name",
                            Values = {"Users"},
                            ShouldMatch = true,
                        });
                    await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = backupPath });
                }


                VerifyDump(backupPath, store =>
                {
                    using (var session = store.OpenSession())
                    {
                        Assert.Equal(1500, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                    }
                });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanPerformDumpWithLimit_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    List<User> generatedUsers;
                    InsertUsers(store, 0, 2000, out generatedUsers);

                    var dumper = new SmugglerDatabaseApi { Options = { Limit = 1500, Incremental = true } };
                    dumper.Options.Filters.Add(
                        new FilterSetting
                        {
                            Path = "@metadata.Raven-Entity-Name",
                            Values = { "Users" },
                            ShouldMatch = true,
                        });

                    await dumper.ExportData(
                        new SmugglerExportOptions<RavenConnectionStringOptions>
                        {
                            From = new RavenConnectionStringOptions {DefaultDatabase = store.DefaultDatabase, Url = "http://localhost:8079"},
                            ToFile = backupPath
                        });
                }

                VerifyDump(backupPath, store =>
                {
                    using (var session = store.OpenSession())
                    {
                        Assert.Equal(1500, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                    }
                });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        private void VerifyDump(string backupPath, Action<EmbeddableDocumentStore> action)
        {
            using (var store = NewDocumentStore())
            {
                var dumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Incremental = true } };
                dumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = backupPath }).Wait();

                action(store);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanPerformDumpWithLimitAndFilter_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    var counter = 0;
                    counter = InsertUsers(store, counter, 1000);
                    counter = InsertDevelopers(store, counter, 2);
                    counter = InsertUsers(store, counter, 1000);
                    InsertDevelopers(store, counter, 2);

                    WaitForIndexing(store);

                    var dumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Limit = 5, Incremental = true } };
                    dumper.Options.Filters.Add(
                        new FilterSetting
                        {
                            Path = "@metadata.Raven-Entity-Name",
                            Values = {"Developers"},
                            ShouldMatch = true,
                        });
                    await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = backupPath });
                }

                VerifyDump(backupPath, store =>
                {
                    using (var session = store.OpenSession())
                    {
                        Assert.Equal(4, session.Query<Developer>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                    }
                });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanPerformDumpWithLimitAndFilter_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    var counter = 0;
                    counter = InsertUsers(store, counter, 1000);
                    counter = InsertDevelopers(store, counter, 2);
                    counter = InsertUsers(store, counter, 1000);
                    InsertDevelopers(store, counter, 2);

                    WaitForIndexing(store);

                    var dumper = new SmugglerDatabaseApi { Options = { Limit = 5, Incremental = true } };
                    dumper.Options.Filters.Add(
                        new FilterSetting
                        {
                            Path = "@metadata.Raven-Entity-Name",
                            Values = {"Developers"},
                            ShouldMatch = true,
                        });
                    await dumper.ExportData(
                        new SmugglerExportOptions<RavenConnectionStringOptions>
                        {
                            ToFile = backupPath,
                            From = new RavenConnectionStringOptions
                            {
                                Url = "http://localhost:8079",
                                DefaultDatabase = store.DefaultDatabase,
                            }
                        });
                }

                VerifyDump(backupPath, store =>
                {
                    using (var session = store.OpenSession())
                    {
                        Assert.Equal(4, session.Query<Developer>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                    }
                });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        private static int InsertDevelopers(IDocumentStore store, int counter, int amount)
        {
            using (var session = store.OpenSession())
            {
                for (var j = 0; j < amount; j++)
                {
                    counter++;
                    session.Store(new Developer { Name = "Developer #" + (counter) });
                }
                session.SaveChanges();
            }
            return counter;
        }

        private static int InsertUsers(IDocumentStore store, int counter, int amount)
        {
            for (var i = 0; i < amount / 25; i++)
            {
                using (var session = store.OpenSession())
                {
                    for (var j = 0; j < 25; j++)
                    {
                        counter++;
                        var user = new User { Name = "User #" + counter };
                        session.Store(user);
                    }
                    session.SaveChanges();
                }
            }
            return counter;
        }

        private static int InsertUsers(IDocumentStore store, int counter, int amount, out List<User> generatedUsers)
        {
            generatedUsers = new List<User>();
            for (var i = 0; i < amount / 25; i++)
            {
                using (var session = store.OpenSession())
                {
                    for (var j = 0; j < 25; j++)
                    {
                        counter++;
                        var user = new User { Name = "User #" + counter };
                        generatedUsers.Add(user);
                        session.Store(user);
                    }
                    session.SaveChanges();
                }
            }
            return counter;
        }


        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpWhenHiddenDocs_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var server = GetNewServer())
                {
                    using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                    {
                        InsertHidenUsers(store, 2000);

                        var user1 = store.DatabaseCommands.Get("users/1");
                        Assert.Null(user1);

                        InsertUsers(store, 1, 25);

                        // now perform full backup
                        var dumper = new DatabaseDataDumper(server.SystemDatabase) { Options = { Incremental = true } };
                        await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = backupPath });
                    }
                }

                VerifyDump(backupPath, store =>
                {
                    using (var session = store.OpenSession())
                    {
                        Assert.Equal(25, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                    }
                });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpWhenHiddenDocs_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (NewRemoteDocumentStore())
                {
                    using (var store = new DocumentStore {Url = "http://localhost:8079"})
                    {
                        store.Initialize();

                        InsertHidenUsers(store, 2000);

                        var user1 = store.DatabaseCommands.Get("users/1");
                        Assert.Null(user1);

                        InsertUsers(store, 1, 25);

                        // now perform full backup
                        var dumper = new SmugglerDatabaseApi { Options = { Incremental = true } };
                        await dumper.ExportData(
                            new SmugglerExportOptions<RavenConnectionStringOptions>
                            {
                                ToFile = backupPath,
                                From = new RavenConnectionStringOptions
                                {
                                    Url = "http://localhost:8079",
                                    DefaultDatabase = store.DefaultDatabase,
                                }
                            });
                    }
                }

                VerifyDump(backupPath, store =>
                {
                    using (var session = store.OpenSession())
                    {
                        Assert.Equal(25, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                    }
                });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpEmptyDatabase_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var server = GetNewServer(databaseName: Constants.SystemDatabase))
                {
                    using (new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                    {
                        // now perform full backup
                        var dumper = new DatabaseDataDumper(server.SystemDatabase) { Options = { Incremental = true } };
                        await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = backupPath });
                    }
                }

                VerifyDump(backupPath, store => Assert.Equal(0, store.SystemDatabase.Documents.GetDocumentsAsJson(0, int.MaxValue, null, CancellationToken.None).Count()));
            }
            finally 
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpEmptyDatabase_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    // now perform full backup
                    var dumper = new SmugglerDatabaseApi { Options = {Incremental = true} };

                    await dumper.ExportData(
                        new SmugglerExportOptions<RavenConnectionStringOptions>
                        {
                            ToFile = backupPath,
                            From = new RavenConnectionStringOptions
                            {
                                Url = "http://localhost:8079",
                                DefaultDatabase = store.DefaultDatabase,
                            }
                        });
                }

                VerifyDump(backupPath, store => Assert.Equal(0, store.SystemDatabase.Documents.GetDocumentsAsJson(0,int.MaxValue, null, CancellationToken.None).Count()));
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);    
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpWhenHiddenDocsWithLimit_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var server = GetNewServer())
            {
                using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                {
                    InsertHidenUsers(store, 2000);

                    var user1 = store.DatabaseCommands.Get("users/1");
                    Assert.Null(user1);

                    InsertUsers(store, 1, 25);

                    // now perform full backup
                    var dumper = new DatabaseDataDumper(server.SystemDatabase) { Options = { Incremental = true } };
                    await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = backupPath });
                }
            }

            VerifyDump(backupPath, store =>
            {
                using (var session = store.OpenSession())
                {
                    Assert.Equal(25, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                }
            });

            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpWhenHiddenDocsWithLimit_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (GetNewServer())
                {
                    using (var store = new DocumentStore { Url = "http://localhost:8079" })
                    {
                        store.Initialize();

                        InsertHidenUsers(store, 2000);

                        var user1 = store.DatabaseCommands.Get("users/1");
                        Assert.Null(user1);

                        InsertUsers(store, 1, 25);

                        // now perform full backup
                        var dumper = new SmugglerDatabaseApi { Options = { Incremental = true } };
                        await dumper.ExportData(
                            new SmugglerExportOptions<RavenConnectionStringOptions>
                            {
                                ToFile = backupPath,
                                From = new RavenConnectionStringOptions
                                {
                                    Url = "http://localhost:8079",
                                    DefaultDatabase = store.DefaultDatabase,
                                }
                            });
                    }
                }

                VerifyDump(backupPath, store =>
                {
                    using (var session = store.OpenSession())
                    {
                        Assert.Equal(25, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                    }
                });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        private static void InsertHidenUsers(IDocumentStore store, int amount)
        {
            for (var i = 0; i < amount; i++)
            {
                store.DatabaseCommands.Put("user/" + (i + 1), null, new RavenJObject(), RavenJObject.FromObject(new
                {
                    hidden = true
                }));
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpAttachments_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    InsertAttachments(store, 328);

                    var dumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Incremental = true, BatchSize = 100 } };
                    await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = backupPath });
                }

                VerifyDump(backupPath, store => Assert.Equal(328, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count()));
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);  
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpAttachments_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    InsertAttachments(store, 328);

                    var dumper = new SmugglerDatabaseApi { Options = { Incremental = true, BatchSize = 100 } };
                    await dumper.ExportData(
                        new SmugglerExportOptions<RavenConnectionStringOptions>
                        {
                            ToFile = backupPath,
                            From = new RavenConnectionStringOptions
                            {
                                Url = "http://localhost:8079",
                                DefaultDatabase = store.DefaultDatabase,
                            }
                        });
                }

                VerifyDump(backupPath, store => Assert.Equal(328, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count()));
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath); 
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpAttachmentsWithLimit_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    InsertAttachments(store, 328);

                    var dumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Incremental = true, BatchSize = 100, Limit = 206 } };
                    await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = backupPath });
                }

                VerifyDump(backupPath, store => Assert.Equal(206, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count()));
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpAttachmentsWithLimit_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    InsertAttachments(store, 328);

                    var dumper = new SmugglerDatabaseApi { Options = { Incremental = true, BatchSize = 100, Limit = 206 } };
                    await dumper.ExportData(
                        new SmugglerExportOptions<RavenConnectionStringOptions>
                        {
                            ToFile = backupPath,
                            From = new RavenConnectionStringOptions
                            {
                                Url = "http://localhost:8079",
                                DefaultDatabase = store.DefaultDatabase,
                            }
                        });
                }

                VerifyDump(backupPath, store => Assert.Equal(206, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count()));
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);                
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpAttachmentsEmpty_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    var dumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Incremental = true, BatchSize = 100, Limit = 206 } };
                    await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = backupPath });
                }

                VerifyDump(backupPath, store => Assert.Equal(0, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count()));
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpAttachmentsEmpty_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    var dumper = new SmugglerDatabaseApi { Options = { Incremental = true, BatchSize = 100, Limit = 206 } };
                    await dumper.ExportData(
                        new SmugglerExportOptions<RavenConnectionStringOptions>
                        {
                            ToFile = backupPath,
                            From = new RavenConnectionStringOptions
                            {
                                Url = "http://localhost:8079",
                                DefaultDatabase = store.DefaultDatabase,
                            }
                        });
                }

                VerifyDump(backupPath, store => Assert.Equal(0, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count()));
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }            
        }

        private static void InsertAttachments(IDocumentStore store, int amount)
        {
            var counter = 0;
            var data = new byte[] { 1, 2, 3, 4 };
            for (var i = 0; i < amount; i++)
            {
                var documentKey = "users/" + (++counter);
                store.DatabaseCommands.PutAttachment(documentKey, null, new MemoryStream(data), new RavenJObject());
            }
        }


        [Fact, Trait("Category", "Smuggler")]
        public async Task CanHandleDocumentExceptionsGracefully_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            var server = GetNewServer(databaseName: Constants.SystemDatabase);

            var alreadyReset = false;

            var port = 8070;
            var forwarder = new ProxyServer(ref port, 8079)
            {
                VetoTransfer = (totalRead, buffer) =>
                {
                    if (alreadyReset == false && totalRead > 25000)
                    {
                        alreadyReset = true;
                        return true;
                    }
                    return false;
                }
            };
            try
            {
                string databaseName;
                using (var store = new DocumentStore
                {
                    Url = "http://localhost:8079"

                })
                {
                    databaseName = store.DefaultDatabase;
                    store.Initialize();
                    InsertUsers(store, 0, 2000);
                }

                var dumper = new SmugglerDatabaseApi { Options = { Limit = 1900, Incremental = true } };
                
                var allDocs = new List<RavenJObject>();

                OperationState exportResult = null;

                try
                {
                    exportResult = await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        ToFile = backupPath,
                        From = new RavenConnectionStringOptions
                        {
                            Url = "http://localhost:" + port,
                            DefaultDatabase = databaseName,
                        }
                    });
                    Assert.False(true, "Previous op should throw.");
                }
                catch (SmugglerExportException e)
                {
                    exportResult = new OperationState
                    {
                        FilePath = e.File
                    };
                }

                using (var fileSteam = new FileStream(exportResult.FilePath, FileMode.Open))
                using (var stream = new GZipStream(fileSteam, CompressionMode.Decompress))
                {
                    var chunk1 = RavenJToken.TryLoad(stream) as RavenJObject;
                    var doc1 = chunk1["Docs"] as RavenJArray;
                    allDocs.AddRange(doc1.Values<RavenJObject>());
                }

                exportResult = await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = backupPath,
                    From = new RavenConnectionStringOptions
                    {
                        Url = "http://localhost:8070",
                        DefaultDatabase = databaseName,
                    }
                });
                using (var fileStream = new FileStream(exportResult.FilePath, FileMode.Open))
                using (var stream = new GZipStream(fileStream, CompressionMode.Decompress))
                {
                    var chunk2 = RavenJToken.TryLoad(stream) as RavenJObject;
                    var doc2 = chunk2["Docs"] as RavenJArray;
                    allDocs.AddRange(doc2.Values<RavenJObject>());
                }

                Assert.Equal(2000, allDocs.Count(d => (d.Value<string>("Name") ?? String.Empty).StartsWith("User")));

            }
            finally
            {
                forwarder.Dispose();
                server.Dispose();
                IOExtensions.DeleteDirectory(backupPath);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanHandleAttachmentExceptionsGracefully_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            var server = GetNewServer();

            int allowDownload = 0;

            var port = 8070;
            var forwarder = new ProxyServer(ref port, 8079)
            {
                VetoTransfer = (totalRead, buffer) =>
                {
                    var payload = System.Text.Encoding.UTF8.GetString(buffer.Array, buffer.Offset, buffer.Count);
                    return payload.Contains("GET /static/users/678 ") && Thread.VolatileRead(ref allowDownload) == 0;
                }
            };
            try
            {
                string databaseName;
                using (var store = new DocumentStore
                {
                    Url = "http://localhost:8079"
                })
                {
                    databaseName = store.DefaultDatabase;
                    store.Initialize();
                    InsertAttachments(store, 2000);
                }

                var dumper = new SmugglerDatabaseApi { Options = { Limit = 1500, Incremental = true } };

                var allAttachments = new List<RavenJObject>();

                OperationState exportResult = null;
                try
                {
                    exportResult = dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        ToFile = backupPath,
                        From = new RavenConnectionStringOptions
                        {
                            Url = "http://localhost:" + port,
                            DefaultDatabase = databaseName,
                        }
                    }).Result;
                    Assert.False(true, "Previous op should throw.");
                }
                catch (AggregateException e)
                {
                    var extractSingleInnerException = e.ExtractSingleInnerException() as SmugglerExportException;
                    if (extractSingleInnerException == null)
                        throw;
                    var inner = extractSingleInnerException;
                    exportResult = new OperationState
                    {
                        FilePath = inner.File
                    };
                }
                Interlocked.Increment(ref allowDownload);

                using (var fileStream = new FileStream(exportResult.FilePath, FileMode.Open))
                using (var stream = new GZipStream(fileStream, CompressionMode.Decompress))
                {
                    var chunk1 = RavenJToken.TryLoad(stream) as RavenJObject;
                    var att1 = chunk1["Attachments"] as RavenJArray;
                    allAttachments.AddRange(att1.Values<RavenJObject>());
                }

                exportResult = await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = backupPath,
                    From = new RavenConnectionStringOptions
                    {
                        Url = "http://localhost:8070",
                        DefaultDatabase = databaseName,
                    }
                });
                using (var fileStream = new FileStream(exportResult.FilePath, FileMode.Open))
                using (var stream = new GZipStream(fileStream, CompressionMode.Decompress))
                {
                    var chunk2 = RavenJToken.TryLoad(stream) as RavenJObject;
                    var attr2 = chunk2["Attachments"] as RavenJArray;
                    allAttachments.AddRange(attr2.Values<RavenJObject>());
                }

                Assert.Equal(2000, allAttachments.Count());
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
                forwarder.Dispose();
                server.Dispose();
            }
        }
    }
}
