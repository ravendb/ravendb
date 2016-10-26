using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Database;
using Raven.Database.Extensions;
using Raven.Database.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using JsonTextWriter = Raven.Imports.Newtonsoft.Json.JsonTextWriter;

namespace Raven.Tests.Smuggler
{
    public class SmugglerExecutionTests : RavenTest
    {
        [Fact, Trait("Category", "Smuggler")]
        public async Task SmugglerShouldThrowIfDatabaseDoesNotExist()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    var connectionStringOptions =
                        new RavenConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultDatabase = "DoesNotExist"
                        };
                    var smuggler = new SmugglerDatabaseApi();

                    var e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ImportData(
                        new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = path, To = connectionStringOptions }));

                    Assert.Equal(string.Format("Smuggler does not support database creation (database 'DoesNotExist' on server '{0}' must exist before running Smuggler).", store.Url), e.Message);

                    e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ExportData(
                        new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = path, From = connectionStringOptions }));

                    Assert.Equal(string.Format("Smuggler does not support database creation (database 'DoesNotExist' on server '{0}' must exist before running Smuggler).", store.Url), e.Message);
                }
            }
            finally
            {
                IOExtensions.DeleteFile(path);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task SmugglerShouldNotThrowIfDatabaseExist1()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("DoesNotExist");

                    var connectionStringOptions = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = "DoesNotExist" };
                    var smuggler = new SmugglerDatabaseApi();

                    await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = path, To = connectionStringOptions });
                    await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = path, From = connectionStringOptions });
                }
            }
            finally
            {
                IOExtensions.DeleteFile(path);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task SmugglerShouldNotThrowIfDatabaseExist2()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    var connectionStringOptions = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = store.DefaultDatabase };
                    var smuggler = new SmugglerDatabaseApi();

                    await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = path, To = connectionStringOptions });
                    await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = path, From = connectionStringOptions });
                }
            }
            finally
            {
                IOExtensions.DeleteFile(path);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task SmugglerBehaviorWhenServerIsDown()
        {
            var path = Path.GetTempFileName();

            try
            {
                var connectionStringOptions = new RavenConnectionStringOptions { Url = "http://localhost:8078/", DefaultDatabase = "DoesNotExist" };
                var smuggler = new SmugglerDatabaseApi();

                var e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ImportData(
                    new SmugglerImportOptions<RavenConnectionStringOptions>
                    {
                        FromFile = path,
                        To = connectionStringOptions
                    }));

                Assert.Contains("Smuggler encountered a connection problem:", e.Message);

                e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ExportData(
                    new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        ToFile = path,
                        From = connectionStringOptions
                    }));

                Assert.Contains("Smuggler encountered a connection problem:", e.Message);
            }
            finally
            {
                IOExtensions.DeleteFile(path);
            }
        }




        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "PeriodicBackup";
        }

        public class User
        {
            public string Name { get; set; }
            public string Id { get; set; }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void CanFullBackupToDirectory()
        {
            var backupPath = NewDataPath("BackupFolder", forceCreateDir: true);
            try
            {
                using (var store = NewDocumentStore())
                {
                    store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }), new RavenJObject());

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "oren" });
                        var periodicBackupSetup = new PeriodicExportSetup
                        {
                            LocalFolderName = backupPath,
                            FullBackupIntervalMilliseconds = 500
                        };
                        session.Store(periodicBackupSetup, PeriodicExportSetup.RavenDocumentKey);

                        session.SaveChanges();
                    }

                    WaitForNextFullBackup(store);
                }
                using (var store = NewDocumentStore())
                {
                    var actualBackupPath = Directory.GetDirectories(backupPath)[0];
                    var fullBackupFilePath = Directory.GetFiles(actualBackupPath).FirstOrDefault(x => x.Contains("full"));
                    Assert.NotNull(fullBackupFilePath);

                    // import the full backup
                    var dataDumper = new DatabaseDataDumper(store.SystemDatabase);
                    dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = fullBackupFilePath }).Wait();

                    using (var session = store.OpenSession())
                    {
                        Assert.Equal("oren", session.Load<User>(1).Name);
                        Assert.NotNull(store.DatabaseCommands.GetAttachment("attach/1"));
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void CanFullBackupToDirectory_MultipleBackups()
        {
            var backupPath = NewDataPath("BackupFolder", forceCreateDir: true);

            try
            {
                string firstFullBackupPath;
                string secondFullBackupPath;
                using (var store = NewDocumentStore())
                {

                    store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }), new RavenJObject());
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "oren" });
                        var periodicBackupSetup = new PeriodicExportSetup
                        {
                            LocalFolderName = backupPath,
                            FullBackupIntervalMilliseconds = 250
                        };
                        session.Store(periodicBackupSetup, PeriodicExportSetup.RavenDocumentKey);

                        session.SaveChanges();
                    }

                    WaitForNextFullBackup(store);
                    firstFullBackupPath = GetLastFullBackupFolder(store);

                    // we have first backup finished here, now insert second object

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "ayende" });
                        session.SaveChanges();
                    }

                    WaitForNextFullBackup(store);
                    secondFullBackupPath = GetLastFullBackupFolder(store);
                }

                AssertUsersCountInBackup(1, Directory.GetFiles(firstFullBackupPath)[0]);
                AssertUsersCountInBackup(2, Directory.GetFiles(secondFullBackupPath)[0]);
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }
        }


        private DateTime? GetLastFullBackupTime(IDocumentStore store)
        {
            var jsonDocument =
                    store.DatabaseCommands.Get(PeriodicExportStatus.RavenDocumentKey);
            if (jsonDocument == null)
                return null;
            var periodicBackupStatus = jsonDocument.DataAsJson.JsonDeserialization<PeriodicExportStatus>();
            return periodicBackupStatus.LastFullBackup;
        }

        private string GetLastFullBackupFolder(IDocumentStore store)
        {
            var jsonDocument =
                    store.DatabaseCommands.Get(PeriodicExportStatus.RavenDocumentKey);
            if (jsonDocument == null)
                return null;
            var periodicBackupStatus = jsonDocument.DataAsJson.JsonDeserialization<PeriodicExportStatus>();
            return periodicBackupStatus.LastFullLocalBackupFolder;
        }

        private void WaitForNextFullBackup(IDocumentStore store)
        {
            var lastFullBackup = DateTime.MinValue;
            var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(5);

            SpinWait.SpinUntil(() =>
            {
                var backupTime = GetLastFullBackupTime(store);
                if (backupTime.HasValue == false)
                    return false;
                if (lastFullBackup == DateTime.MinValue)
                {
                    lastFullBackup = backupTime.Value;
                    return false;
                }
                return lastFullBackup != backupTime;
            }, timeout);
        }

        private void AssertUsersCountInBackup(int expectedNumberOfUsers, string file)
        {
            using (var store = NewDocumentStore())
            {
                var dataDumper = new DatabaseDataDumper(store.SystemDatabase);
                dataDumper.Options.Incremental = false;
                dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = file }).Wait();

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(expectedNumberOfUsers, session.Query<User>().Count());
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void SmugglerCanUnderstandPeriodicBackupFormat()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                string userId;
                using (var session = store.OpenSession())
                {
                    var periodicBackupSetup = new PeriodicExportSetup
                    {
                        LocalFolderName = backupPath,
                        IntervalMilliseconds = 100
                    };
                    session.Store(periodicBackupSetup, PeriodicExportSetup.RavenDocumentKey);

                    session.SaveChanges();
                }

                var backupStatus = GetPeriodicBackupStatus(store.SystemDatabase);

                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "oren" };
                    session.Store(user);
                    userId = user.Id;
                    session.SaveChanges();
                }
                store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3, 4 }), new RavenJObject());

                WaitForPeriodicExport(store.SystemDatabase, backupStatus,PeriodicExportStatus.PeriodicExportStatusEtags.LastAttachmentsEtag);

                store.DatabaseCommands.Delete(userId, null);
                store.DatabaseCommands.DeleteAttachment("attach/1", null);

                WaitForPeriodicExport(store.SystemDatabase, backupStatus, PeriodicExportStatus.PeriodicExportStatusEtags.LastAttachmentDeletionEtag);

            }

            using (var store = NewRemoteDocumentStore())
            {
                var dataDumper = new SmugglerDatabaseApi();
                dataDumper.Options.Incremental = true;
                dataDumper.ImportData(
                    new SmugglerImportOptions<RavenConnectionStringOptions>
                    {
                        FromFile = backupPath,
                        To = new RavenConnectionStringOptions { Url = store.Url }
                    }).Wait();

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<User>(1));
                    Assert.Null(store.DatabaseCommands.GetAttachment("attach/1"));
                }
            }

            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact, Trait("Category", "Smuggler")]
        public void CanBackupDocumentDeletion()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                string userId;
                using (var session = store.OpenSession())
                {
                    var periodicBackupSetup = new PeriodicExportSetup
                    {
                        LocalFolderName = backupPath,
                        IntervalMilliseconds = 100
                    };
                    session.Store(periodicBackupSetup, PeriodicExportSetup.RavenDocumentKey);

                    session.SaveChanges();
                }

                var backupStatus = GetPeriodicBackupStatus(store.SystemDatabase);

                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "oren" };
                    session.Store(user);
                    userId = user.Id;
                    session.SaveChanges();
                }

                WaitForPeriodicExport(store.SystemDatabase, backupStatus);

                store.DatabaseCommands.Delete(userId, null);

                WaitForPeriodicExport(store.SystemDatabase, backupStatus, x => x.LastDocsDeletionEtag);

            }

            using (var store = NewDocumentStore())
            {
                var dataDumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Incremental = true } };
                dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = backupPath }).Wait();

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<User>(1));
                }
            }

            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact, Trait("Category", "Smuggler")]
        public void CanBackupAttachmentDeletion()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var periodicBackupSetup = new PeriodicExportSetup
                    {
                        LocalFolderName = backupPath,
                        IntervalMilliseconds = 250
                    };
                    session.Store(periodicBackupSetup, PeriodicExportSetup.RavenDocumentKey);

                    session.SaveChanges();
                }

                var backupStatus = GetPeriodicBackupStatus(store.SystemDatabase);

                store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3, 4 }), new RavenJObject());

                WaitForPeriodicExport(store.SystemDatabase, backupStatus, PeriodicExportStatus.PeriodicExportStatusEtags.LastAttachmentsEtag);

                store.DatabaseCommands.DeleteAttachment("attach/1", null);

                WaitForPeriodicExport(store.SystemDatabase, backupStatus, PeriodicExportStatus.PeriodicExportStatusEtags.LastAttachmentDeletionEtag);

            }

            using (var store = NewDocumentStore())
            {
                var dataDumper = new DatabaseDataDumper(store.SystemDatabase) { Options = { Incremental = true } };
                dataDumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = backupPath }).Wait();

                Assert.Null(store.DatabaseCommands.GetAttachment("attach/1"));
            }

            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public void ShouldDeleteDocumentTombStoneAfterNextPut()
        {
            using (EmbeddableDocumentStore store = NewDocumentStore())
            {
                // create document
                string userId;
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "oren" };
                    session.Store(user);
                    userId = user.Id;
                    session.SaveChanges();
                }

                //now delete it and check for tombstone
                using (var session = store.OpenSession())
                {
                    session.Delete(userId);
                    session.SaveChanges();
                }

                store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, userId);
                    Assert.NotNull(tombstone);
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "ayende" }, userId);
                    session.SaveChanges();
                }

                store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, userId);
                    Assert.Null(tombstone);
                });

            }
        }

        [Fact]
        public void ShouldDeleteAttachmentTombStoneAfterNextPut()
        {
            using (EmbeddableDocumentStore store = NewDocumentStore())
            {
                // create document
                store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }), new RavenJObject());

                //now delete it and check for tombstone
                store.DatabaseCommands.DeleteAttachment("attach/1", null);

                store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, "attach/1");
                    Assert.NotNull(tombstone);
                });

                store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }), new RavenJObject());

                store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, "attach/1");
                    Assert.Null(tombstone);
                });
            }
        }

        [Fact]
        public void CanDeleteTombStones()
        {
            using (var store = NewRemoteDocumentStore(databaseName: Constants.SystemDatabase))
            {
                string userId;
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "oren" };
                    session.Store(user);
                    userId = user.Id;
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(userId);
                    session.SaveChanges();
                }

                store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }), new RavenJObject());
                
                store.DatabaseCommands.DeleteAttachment("attach/1", null);
                
                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                                                                     Assert.Equal(1,
                                                                                  accessor.Lists.Read(
                                                                                      Constants
                                                                                          .RavenPeriodicExportsDocsTombstones,
                                                                                      Etag.Empty, null, 10).Count()));

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(1, accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, Etag.Empty, null, 10).Count()));

                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "oren" };
                    session.Store(user);
                    userId = user.Id;
                    session.SaveChanges();
                }
                store.DatabaseCommands.PutAttachment("attach/2", null, new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }), new RavenJObject());

                var documentEtagAfterFirstDelete = Etag.Empty;
                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor => documentEtagAfterFirstDelete = accessor.Staleness.GetMostRecentDocumentEtag());

                var attachmentEtagAfterFirstDelete = Etag.Empty;
                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor => attachmentEtagAfterFirstDelete = accessor.Staleness.GetMostRecentAttachmentEtag());

                using (var session = store.OpenSession())
                {
                    session.Delete(userId);
                    session.SaveChanges();
                }

                store.DatabaseCommands.DeleteAttachment("attach/2", null);


                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(2, accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, Etag.Empty, null, 10).Count()));

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(2, accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, Etag.Empty, null, 10).Count()));

                var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null,
                                                                    servers[0].SystemDatabase.ServerUrl +
                                                                    "admin/periodicExport/purge-tombstones?docEtag=" + documentEtagAfterFirstDelete + "&attachmentEtag=" + attachmentEtagAfterFirstDelete,
                                                                    HttpMethods.Post,
                                                                    new OperationCredentials(null, CredentialCache.DefaultCredentials),
                                                                    store.Conventions);

                store.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ReadResponseJson();

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(1, accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, Etag.Empty, null, 10).Count()));
                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(1, accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, Etag.Empty, null, 10).Count()));

            }
        }

        [Fact]
        public void PeriodicBackupDoesntProduceExcessiveFilesAndCleanupTombstonesProperly()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        var periodicBackupSetup = new PeriodicExportSetup
                        {
                            LocalFolderName = backupPath,
                            IntervalMilliseconds = 2000,
                            FullBackupIntervalMilliseconds = 5000
                        };
                        session.Store(periodicBackupSetup, PeriodicExportSetup.RavenDocumentKey);


                        session.SaveChanges();
                    }

                    var backupStatus = GetPeriodicBackupStatus(store.SystemDatabase);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "oren" });
                        session.Store(new User { Name = "ayende" });
                        store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }), new RavenJObject());
                        store.DatabaseCommands.PutAttachment("attach/2", null, new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }), new RavenJObject());
                        session.SaveChanges();
                    }

                    WaitForPeriodicExport(store.SystemDatabase, backupStatus);
                    
                    var actualBackupPath = Directory.GetDirectories(backupPath)[0];

                    // one full export
                    VerifyFullBackupExists(actualBackupPath);

                    store.DatabaseCommands.Delete("users/1", null);
                    store.DatabaseCommands.Delete("users/2", null);
                    store.DatabaseCommands.DeleteAttachment("attach/1", null);
                    store.DatabaseCommands.DeleteAttachment("attach/2", null);

                    store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                    {
                        Assert.Equal(2,
                            accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, Etag.Empty, null, 20)
                                .Count());
                        Assert.Equal(2,
                            accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, Etag.Empty, null, 20)
                                .Count());
                    });


                    WaitForPeriodicExport(store.SystemDatabase, backupStatus);

                    // status + two exports (one full, one incremental)
                    VerifyFilesCount(1 + 2, actualBackupPath);
                    

                    store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                    {
                        Assert.True(accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, Etag.Empty, null, 20).Any());
                        Assert.True(accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, Etag.Empty, null, 20).Any());
                    });

                }
            }
            finally
            {
                try
                {
                    IOExtensions.DeleteDirectory(backupPath);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
        }

        private void VerifyFilesCount(int expectedFiles, string backupPath)
        {
            Assert.Equal(expectedFiles, Directory.GetFiles(backupPath).Count());
        }

        private void VerifyFullBackupExists(string backupPath)
        {
            Assert.True(Directory.GetFiles(backupPath).Any(x=>x.Contains(".ravendb-full-dump")));
        }

        /// <summary>
        ///  In 2.5 we didn't support deleted documents/attachments, so those props aren't available. 
        /// </summary>
        [Fact]
        public void CanProperlyReadLastEtagUsingPreviousFormat()
        {
            var backupPath = NewDataPath("BackupFolder", forceCreateDir: true);

            var etagFileLocation = Path.Combine(Path.GetDirectoryName(backupPath), "IncrementalExport.state.json");
            using (var streamWriter = new StreamWriter(File.Create(etagFileLocation)))
            {
                new RavenJObject
                    {
                        {"LastDocEtag", Etag.Parse("00000000-0000-0000-0000-000000000001").ToString()},
                        {"LastAttachmentEtag", Etag.Parse("00000000-0000-0000-0000-000000000002").ToString()}
                    }.WriteTo(new JsonTextWriter(streamWriter));
                streamWriter.Flush();
            }

            var result = new OperationState
            {
                FilePath = backupPath
            };
            SmugglerDatabaseApiBase.ReadLastEtagsFromFile(result);

            Assert.Equal("00000000-0000-0000-0000-000000000001", result.LastDocsEtag.ToString());
            Assert.Equal("00000000-0000-0000-0000-000000000002", result.LastAttachmentsEtag.ToString());
            Assert.Equal(Etag.Empty, result.LastDocDeleteEtag);
            Assert.Equal(Etag.Empty, result.LastAttachmentsDeleteEtag);
        }

        /// <summary>
        /// Purpose of this class is to expose few protected method and make them easily testable without using reflection.
        /// </summary>
        public class CustomDataDumper : DatabaseDataDumper
        {
            public CustomDataDumper(DocumentDatabase database)
                : base(database)
            {
            }

            public Task<ExportOperationStatus> ExportDocuments(SmugglerJsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag, int maxNumberOfDocumentsToExport)
            {
                Operations.Initialize(Options);

                return ExportDocuments(new RavenConnectionStringOptions(), jsonWriter, lastEtag, maxEtag, maxNumberOfDocumentsToExport);
            }

            public Task<ExportOperationStatus> ExportAttachments(SmugglerJsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag, int maxNumberOfAttachmentsToExport)
            {
                Operations.Initialize(Options);

                return ExportAttachments(new RavenConnectionStringOptions(), jsonWriter, lastEtag, maxEtag, maxNumberOfAttachmentsToExport);
            }
        }

        [Fact]
        public async Task DataDumperExportHandlesMaxEtagCorrectly()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new User { Name = "oren #" + (i + 1) });
                    }
                    session.SaveChanges();
                }

                using (var textStream = new StringWriter())
                using (var writer = new SmugglerJsonTextWriter(textStream))
                {
                    var dumper = new CustomDataDumper(store.SystemDatabase);

                    var startEtag = store.SystemDatabase.Statistics.LastDocEtag.IncrementBy(-5);
                    var endEtag = startEtag.IncrementBy(2);

                    writer.WriteStartArray();
                    var status = await dumper.ExportDocuments(writer, startEtag, endEtag, int.MaxValue);
                    writer.WriteEndArray();
                    writer.Flush();

                    // read exported content
                    var exportedDocs = RavenJArray.Parse(textStream.GetStringBuilder().ToString());
                    Assert.Equal(2, exportedDocs.Count());

                    Assert.Equal("01000000-0000-0001-0000-000000000007", exportedDocs.First().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                    Assert.Equal("01000000-0000-0001-0000-000000000008", exportedDocs.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                    Assert.Equal("01000000-0000-0001-0000-000000000008", status.LastEtag.ToString());

                }

                using (var textStream = new StringWriter())
                using (var writer = new SmugglerJsonTextWriter(textStream))
                {
                    var dumper = new CustomDataDumper(store.SystemDatabase);

                    var startEtag = store.SystemDatabase.Statistics.LastDocEtag.IncrementBy(-5);

                    writer.WriteStartArray();
                    var status = await dumper.ExportDocuments(writer, startEtag, null, int.MaxValue);
                    writer.WriteEndArray();
                    writer.Flush();

                    // read exported content
                    var exportedDocs = RavenJArray.Parse(textStream.GetStringBuilder().ToString());
                    Assert.Equal(5, exportedDocs.Count());

                    Assert.Equal("01000000-0000-0001-0000-000000000007", exportedDocs.First().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                    Assert.Equal("01000000-0000-0001-0000-00000000000B", exportedDocs.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                    Assert.Equal("01000000-0000-0001-0000-00000000000B", status.LastEtag.ToString());
                }

                for (var i = 0; i < 10; i++)
                {
                    store.DatabaseCommands.PutAttachment("attach/" + (i + 1), null, new MemoryStream(new[] { (byte)i }), new RavenJObject());
                }

                using (var textStream = new StringWriter())
                using (var writer = new SmugglerJsonTextWriter(textStream))
                {
                    var dumper = new CustomDataDumper(store.SystemDatabase);

                    var startEtag = store.SystemDatabase.Statistics.LastAttachmentEtag.IncrementBy(-5);
                    var endEtag = startEtag.IncrementBy(2);

                    writer.WriteStartArray();
                    var status = await dumper.ExportAttachments(writer, startEtag, endEtag, int.MaxValue);
                    writer.WriteEndArray();
                    writer.Flush();

                    // read exported content
                    var exportedAttachments = RavenJArray.Parse(textStream.GetStringBuilder().ToString());
                    Assert.Equal(2, exportedAttachments.Count());

                    Assert.Equal("02000000-0000-0001-0000-000000000006", exportedAttachments.First().Value<string>("Etag"));
                    Assert.Equal("02000000-0000-0001-0000-000000000007", exportedAttachments.Last().Value<string>("Etag"));
                    Assert.Equal("02000000-0000-0001-0000-000000000007", status.LastEtag.ToString());

                }

                using (var textStream = new StringWriter())
                using (var writer = new SmugglerJsonTextWriter(textStream))
                {
                    var dumper = new CustomDataDumper(store.SystemDatabase);

                    var startEtag = store.SystemDatabase.Statistics.LastAttachmentEtag.IncrementBy(-5);

                    writer.WriteStartArray();
                    var status = await dumper.ExportAttachments(writer, startEtag, null, int.MaxValue);
                    writer.WriteEndArray();
                    writer.Flush();

                    // read exported content
                    var exportedAttachments = RavenJArray.Parse(textStream.GetStringBuilder().ToString());
                    Assert.Equal(5, exportedAttachments.Count());

                    Assert.Equal("02000000-0000-0001-0000-000000000006", exportedAttachments.First().Value<string>("Etag"));
                    Assert.Equal("02000000-0000-0001-0000-00000000000A", exportedAttachments.Last().Value<string>("Etag"));
                    Assert.Equal("02000000-0000-0001-0000-00000000000A", status.LastEtag.ToString());

                }

                WaitForIndexing(store);

                store.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery()
                {
                    Query = "Tag:Users"
                }).WaitForCompletion();

                for (var i = 0; i < 10; i++)
                {
                    store.DatabaseCommands.DeleteAttachment("attach/" + (i + 1), null);
                }

                Etag user6DeletionEtag = null, user9DeletionEtag = null, attach5DeletionEtag = null, attach7DeletionEtag = null;

               //WaitForUserToContinueTheTest(url:store.Url);

                store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                {
                    user6DeletionEtag =
                        accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, "users/6").Etag;
                    user9DeletionEtag =
                        accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, "users/9").Etag;
                    attach5DeletionEtag =
                        accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, "attach/5").Etag;
                    attach7DeletionEtag =
                        accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, "attach/7").Etag;

                });

                using (var textStream = new StringWriter())
                using (var writer = new SmugglerJsonTextWriter(textStream))
                {
                    var dumper = new CustomDataDumper(store.SystemDatabase);

                    writer.WriteStartObject();
                    var exportResult = new OperationState
                    {
                        LastDocDeleteEtag = user6DeletionEtag,
                        LastAttachmentsDeleteEtag = attach5DeletionEtag
                    };

                    dumper.ExportDeletions(writer, exportResult, new LastEtagsInfo { LastDocDeleteEtag = user9DeletionEtag, LastAttachmentsDeleteEtag = attach7DeletionEtag }).Wait();
                    writer.WriteEndObject();
                    writer.Flush();

                    // read exported content
                    var exportJson = RavenJObject.Parse(textStream.GetStringBuilder().ToString());
                    var docsKeys =
                        exportJson.Value<RavenJArray>("DocsDeletions").Select(x => x.Value<string>("Key")).ToArray();
                    var attachmentsKeys =
                        exportJson.Value<RavenJArray>("AttachmentsDeletions")
                                  .Select(x => x.Value<string>("Key"))
                                  .ToArray();
                    Assert.Equal(new[] { "users/7", "users/8", "users/9" }, docsKeys);
                    Assert.Equal(new[] { "attach/6", "attach/7" }, attachmentsKeys);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task MaxNumberOfItemsToProcessInSingleBatchShouldBeRespectedByDataDumper()
        {
            var path = Path.Combine(NewDataPath(forceCreateDir: true), "raven.dump");

            using (var server = GetNewServer(configureConfig: configuration => configuration.MaxNumberOfItemsToProcessInSingleBatch = 1234))
            {
                var dumper = new DatabaseDataDumper(server.SystemDatabase, options: new SmugglerDatabaseOptions { BatchSize = 4321 });
                Assert.Equal(4321, dumper.Options.BatchSize);

                await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = path });

                Assert.Equal(1234, dumper.Options.BatchSize);

                dumper = new DatabaseDataDumper(server.SystemDatabase, options: new SmugglerDatabaseOptions { BatchSize = 4321 });
                Assert.Equal(4321, dumper.Options.BatchSize);

                dumper.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = path }).Wait();

                Assert.Equal(1234, dumper.Options.BatchSize);

                dumper = new DatabaseDataDumper(server.SystemDatabase, options: new SmugglerDatabaseOptions { BatchSize = 1000 });
                Assert.Equal(1000, dumper.Options.BatchSize);

                await dumper.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = path });

                Assert.Equal(1000, dumper.Options.BatchSize);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task MaxNumberOfItemsToProcessInSingleBatchShouldBeRespectedBySmuggler()
        {
            var path = Path.Combine(NewDataPath(forceCreateDir: true), "raven.dump");

            using (var server = GetNewServer(configureConfig: configuration => configuration.MaxNumberOfItemsToProcessInSingleBatch = 1234))
            {
                var smuggler = new SmugglerDatabaseApi(options: new SmugglerDatabaseOptions { BatchSize = 4321 });
                Assert.Equal(4321, smuggler.Options.BatchSize);

                await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = path, From = new RavenConnectionStringOptions { Url = server.Configuration.ServerUrl } });

                Assert.Equal(1234, smuggler.Options.BatchSize);

                smuggler = new SmugglerDatabaseApi(options: new SmugglerDatabaseOptions { BatchSize = 4321 });
                Assert.Equal(4321, smuggler.Options.BatchSize);

                await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = path, To = new RavenConnectionStringOptions { Url = server.Configuration.ServerUrl } });

                Assert.Equal(1234, smuggler.Options.BatchSize);

                smuggler = new SmugglerDatabaseApi(options: new SmugglerDatabaseOptions { BatchSize = 1000 });
                Assert.Equal(1000, smuggler.Options.BatchSize);

                await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = path, From = new RavenConnectionStringOptions { Url = server.Configuration.ServerUrl } });

                Assert.Equal(1000, smuggler.Options.BatchSize);
            }
        }

        private class BigDocument
        {
            private static Random generator = new Random();
            public string Id { get; set; }

            public byte[] Payload { get; set; }

            public BigDocument( int payloadSize )
            {
                byte[] value = new byte[payloadSize];
                generator.NextBytes(value);

                this.Payload = value;
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanSkipFilesWhenUsingContinuations()
        {
            var dataDir = NewDataPath(forceCreateDir: true);
            const string continuationToken = "Token";

            using (var store = NewRemoteDocumentStore( dataDirectory: dataDir ))
            {
                store.Conventions.MaxNumberOfRequestsPerSession = 1000;

                // Prepare everything.
                int serverPort = new Uri(store.Url).Port;
                var server = GetServer(serverPort);
                var outputDirectory = Path.Combine(server.Configuration.DataDirectory, "Export");

                string newDatabase = store.DefaultDatabase + "-Verify";
                await store.AsyncDatabaseCommands.EnsureDatabaseExists(newDatabase);
                
                
                // Prepare the first batch of documents on incremental setup.
                var storedDocuments = new List<BigDocument>();
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var document = new BigDocument(3000);

                        storedDocuments.Add(document);
                        session.Store(document);
                    }
                    session.SaveChanges();
                }

                // Export the first batch
                var dumper = new SmugglerDatabaseApi { Options = { Incremental = true } };
                var exportResult = await dumper.ExportData(
                    new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        ToFile = outputDirectory,
                        From = new RavenConnectionStringOptions
                        {
                            Url = "http://localhost:" + serverPort,
                            DefaultDatabase = store.DefaultDatabase,
                        }
                    });

                Assert.NotNull(exportResult);
                Assert.True(!string.IsNullOrWhiteSpace(exportResult.FilePath));


                // Import the first batch
                var importDumper = new SmugglerDatabaseApi { Options = { Incremental = true, ContinuationToken = continuationToken, BatchSize = 1 } };
                await importDumper.ImportData(
                        new SmugglerImportOptions<RavenConnectionStringOptions>
                        {
                            FromFile = outputDirectory,
                            To = new RavenConnectionStringOptions
                            {
                                Url = "http://localhost:" + serverPort,
                                DefaultDatabase = newDatabase,
                            }
                        });

                // Store the etags of the first batch to ensure we are not touching them (effectively skipping).
                var docMap = new Dictionary<string, Etag>();
                using (var session = store.OpenSession(newDatabase))
                {
                    foreach (var sdoc in storedDocuments)
                    {
                        var doc = session.Load<BigDocument>(sdoc.Id);
                        Assert.NotNull(doc);

                        var etag = session.Advanced.GetEtagFor<BigDocument>(doc);
                        docMap[doc.Id] = etag;
                    }
                }

                // Prepare the second batch of documents on incremental setup.
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        var document = new BigDocument(1000);

                        storedDocuments.Add(document);
                        session.Store(document);
                    }
                    session.SaveChanges();
                }

                // Export the second batch.
                exportResult = await dumper.ExportData(
                    new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        ToFile = outputDirectory,
                        From = new RavenConnectionStringOptions
                        {
                            Url = "http://localhost:" + serverPort,
                            DefaultDatabase = store.DefaultDatabase,
                        }
                    });


                // Importing the second batch effectively skipping the batch already imported.
                await importDumper.ImportData(
                        new SmugglerImportOptions<RavenConnectionStringOptions>
                        {
                            FromFile = outputDirectory,
                            To = new RavenConnectionStringOptions
                            {
                                Url = "http://localhost:" + serverPort,
                                DefaultDatabase = newDatabase,
                            }
                        });
              
                using (var session = store.OpenSession(newDatabase))
                {
                    foreach (var doc in storedDocuments)
                    {
                        var storedDoc = session.Load<BigDocument>(doc.Id);
                        Assert.NotNull(storedDoc);

                        Etag etag;
                        if (docMap.TryGetValue(doc.Id, out etag))
                            Assert.Equal(etag, session.Advanced.GetEtagFor(storedDoc));
                    }
                }
            }
        }
  
        private string CreateTestExportFile(int docFirstId, int numberOfDocs)
        {
            string exportString1 = @"
{
   ""Indexes"": [
                  {
      ""name"": ""Raven/DocumentsByEntityName"",
                    ""definition"": {
        ""IndexId"": 1,
                      ""Name"": ""Raven/DocumentsByEntityName"",
                      ""LockMode"": ""LockedIgnore"",
                      ""IndexVersion"": 0,
                      ""Map"": ""from doc in docs \r\nselect new \r\n{ \r\n    Tag = doc[\""@metadata\""][\""Raven-Entity-Name\""], \r\n    LastModified = (DateTime)doc[\""@metadata\""][\""Last-Modified\""],\r\n    LastModifiedTicks = ((DateTime)doc[\""@metadata\""][\""Last-Modified\""]).Ticks \r\n};"",
                      ""Maps"": [
                        ""from doc in docs \r\nselect new \r\n{ \r\n    Tag = doc[\""@metadata\""][\""Raven-Entity-Name\""], \r\n    LastModified = (DateTime)doc[\""@metadata\""][\""Last-Modified\""],\r\n    LastModifiedTicks = ((DateTime)doc[\""@metadata\""][\""Last-Modified\""]).Ticks \r\n};""
        ],
        ""Reduce"": null,
        ""IsMapReduce"": false,
        ""IsCompiled"": false,
        ""Stores"": {},
        ""Indexes"": {
          ""Tag"": ""NotAnalyzed"",
          ""LastModified"": ""NotAnalyzed"",
          ""LastModifiedTicks"": ""NotAnalyzed""
        },
        ""SortOptions"": {
          ""LastModified"": ""String"",
          ""LastModifiedTicks"": ""Long""
        },
        ""Analyzers"": {},
        ""Fields"": [
          ""Tag"",
          ""LastModified"",
          ""LastModifiedTicks"",
          ""__document_id""
        ],
        ""Suggestions"": null,
        ""SuggestionsOptions"": [],
        ""TermVectors"": {},
        ""SpatialIndexes"": {},
        ""InternalFieldsMapping"": {},
        ""MaxIndexOutputsPerDocument"": null,
        ""Type"": ""Map"",
        ""DisableInMemoryIndexing"": true,
        ""IsTestIndex"": false,
        ""IsSideBySideIndex"": false
    }
}
  ],            
  ""Docs"": [";

            var docStringPart1 = @"
    {
      ""Smuggler"": ""Test_";

            var docStringPart2 = @""",
      ""@metadata"": {
        ""Raven-Entity-Name"": ""testdoc"",
        ""@id"": ""testdoc/";

            var docStringPart3 = @""",
        ""Last-Modified"": ""2015-11-27T23:33:27.8437754Z"",
        ""Raven-Last-Modified"": ""2015-11-27T23:33:27.8437754"",
        ""@etag"": ""01000000-0000-0001-0000-000000000001"",
        ""Non-Authoritative-Information"": false
      }
    }";
            var exportString2 = @"
  ],
  ""Attachments"": [],
  ""Transformers"": [],
  ""Identities"": []
}'";
            var fileToExportTo = Path.GetTempFileName();
            var fileStream = File.OpenWrite(fileToExportTo);
            using (var gzipStream = new GZipStream(fileStream, CompressionMode.Compress, leaveOpen: true))
            using (var streamWriter = new StreamWriter(gzipStream))
            {
                streamWriter.Write(exportString1);
                int docs;
                // 200K docs should produce about 2MB of compressed export file
                for (docs = docFirstId; docs < numberOfDocs; docs++)
                {
                    streamWriter.Write($"{docStringPart1}{docs}{docStringPart2}{docs}{docStringPart3},");
                }
                streamWriter.Write($"{docStringPart1}{docs}{docStringPart2}{docs}{docStringPart3}");
                streamWriter.Write(exportString2);
                streamWriter.Flush();
            }
            fileStream.Flush();
            fileStream.Close();
            fileStream.Dispose();

            return fileToExportTo;
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanExportImportSmugglerMaxSplitExportFileSize()
        {
            var fileToExportTo = CreateTestExportFile(1, 150000); 
            var splittedFileToExportTo = Path.GetTempFileName();
            Assert.NotNull(splittedFileToExportTo);
            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    var connectionStringOptions = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = store.DefaultDatabase };
                    var smuggler = new SmugglerDatabaseApi();

                    // import one file:
                    await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = fileToExportTo, To = connectionStringOptions });

                    // export as two files:
                    await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = splittedFileToExportTo, From = connectionStringOptions, MaxSplitExportFileSize = 1 });

                    Assert.True(File.Exists(splittedFileToExportTo));
                    Assert.True(File.Exists($"{splittedFileToExportTo}.part001"));
                    Assert.False(File.Exists($"{splittedFileToExportTo}.part002"));

                    using (var session = store.OpenSession())
                    {
                        session.Delete("testdoc/149997");
                        session.SaveChanges();
                        var o = session.Load<object>("testdoc/149997");
                        Assert.Null(o);
                    }

                    // import two files:
                    await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = splittedFileToExportTo, To = connectionStringOptions });

                    using (var session = store.OpenSession())
                    {
                        var o = session.Load<object>("testdoc/149997");
                        Assert.NotNull(o);
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteFile(splittedFileToExportTo);
                IOExtensions.DeleteFile($"{splittedFileToExportTo}.part001");
                IOExtensions.DeleteFile(fileToExportTo);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanExportImportIncrementalSmugglerMaxSplitExportFileSize()
        {
            var fileToExportTo = CreateTestExportFile(1, 10000);
            var fileToExportIncrementalTo = CreateTestExportFile(10001, 150000);
            var mainFileToExportTo = Path.GetTempFileName();
            Assert.NotNull(mainFileToExportTo);
            var directoryToExportIncrementalTo =
                Path.Combine(Path.GetDirectoryName(mainFileToExportTo), "TestIncremental");
            Directory.CreateDirectory(directoryToExportIncrementalTo);
            string secondaryFileToExportTo = null;
            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    var connectionStringOptions = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = store.DefaultDatabase };
                    var smuggler = new SmugglerDatabaseApi();

                    // import one file:
                    await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = fileToExportTo, To = connectionStringOptions });

                    // export as one files:
                    await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = mainFileToExportTo, From = connectionStringOptions });
                    Assert.True(File.Exists(mainFileToExportTo));

                    // export incremental as two files:
                    await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = fileToExportIncrementalTo, To = connectionStringOptions });
                    var operState = await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> { ToFile = directoryToExportIncrementalTo, From = connectionStringOptions, MaxSplitExportFileSize = 1, IsIncrementalExport = true});

                    secondaryFileToExportTo = operState.FilePath;
                    Assert.True(File.Exists(secondaryFileToExportTo));
                    Assert.True(File.Exists($"{secondaryFileToExportTo}.part001"));
                    Assert.False(File.Exists($"{secondaryFileToExportTo}.part002"));

                    using (var session = store.OpenSession())
                    {
                        session.Delete("testdoc/20000");
                        session.Delete("testdoc/149997");
                        session.SaveChanges();
                        var o = session.Load<object>("testdoc/20000");
                        Assert.Null(o);
                        o = session.Load<object>("testdoc/149997");
                        Assert.Null(o);
                    }

                    // import two files:
                    await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions> { FromFile = directoryToExportIncrementalTo, To = connectionStringOptions, IsIncrementalImport = true});

                    using (var session = store.OpenSession())
                    {
                        var o = session.Load<object>("testdoc/20000");
                        Assert.NotNull(o);
                        o = session.Load<object>("testdoc/149997");
                        Assert.NotNull(o);
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteFile(fileToExportTo);
                IOExtensions.DeleteFile(fileToExportIncrementalTo);
                IOExtensions.DeleteFile(mainFileToExportTo);
                if (secondaryFileToExportTo != null)
                {
                    IOExtensions.DeleteFile(secondaryFileToExportTo);
                    IOExtensions.DeleteFile($"{secondaryFileToExportTo}.part001");
                }
                IOExtensions.DeleteDirectory(directoryToExportIncrementalTo);
            }
        }
    }
}
