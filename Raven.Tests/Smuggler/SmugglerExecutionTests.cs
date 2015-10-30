using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Database.Smuggler.Embedded;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Files;
using Raven.Smuggler.Database.Remote;
using Raven.Smuggler.Database.Streams;

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
                    var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = store.Url,
                        Database = "DoesNotExist"
                    };

                    var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileSource(path), new DatabaseSmugglerRemoteDestination(connectionOptions));

                    var e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ExecuteAsync());

                    Assert.Equal(string.Format("Smuggler does not support database creation (database 'DoesNotExist' on server '{0}' must exist before running Smuggler).", store.Url), e.Message);

                    smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerRemoteSource(connectionOptions), new DatabaseSmugglerFileDestination(path));

                    e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ExecuteAsync());

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

                    var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = store.Url,
                        Database = "DoesNotExist"
                    };

                    var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileSource(path), new DatabaseSmugglerRemoteDestination(connectionOptions));
                    await smuggler.ExecuteAsync();

                    smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerRemoteSource(connectionOptions), new DatabaseSmugglerFileDestination(path));
                    await smuggler.ExecuteAsync();
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
                    var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = store.Url,
                        Database = store.DefaultDatabase
                    };

                    var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileSource(path), new DatabaseSmugglerRemoteDestination(connectionOptions));
                    await smuggler.ExecuteAsync();

                    smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerRemoteSource(connectionOptions), new DatabaseSmugglerFileDestination(path));
                    await smuggler.ExecuteAsync();
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
                var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions
                {
                    Url = "http://localhost:8078/",
                    Database = "DoesNotExist"
                };

                var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileSource(path), new DatabaseSmugglerRemoteDestination(connectionOptions));

                var e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ExecuteAsync());

                Assert.Contains("Smuggler encountered a connection problem:", e.Message);

                smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerRemoteSource(connectionOptions), new DatabaseSmugglerFileDestination(path));

                e = await AssertAsync.Throws<SmugglerException>(() => smuggler.ExecuteAsync());

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
                    var backupFile = Directory.GetFiles(Path.GetFullPath(backupPath))
                        .Where(file => ".ravendb-full-dump".Equals(Path.GetExtension(file), StringComparison.InvariantCultureIgnoreCase))
                        .OrderBy(File.GetLastWriteTimeUtc)
                        .First();

                    var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileSource(backupFile), new DatabaseSmugglerEmbeddedDestination(store.SystemDatabase));
                    smuggler.Execute();

                    using (var session = store.OpenSession())
                    {
                        Assert.Equal("oren", session.Load<User>(1).Name);
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
                using (var store = NewDocumentStore())
                {
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

                    // we have first backup finished here, now insert second object

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "ayende" });
                        session.SaveChanges();
                    }

                    WaitForNextFullBackup(store);
                }


                var files = Directory.GetFiles(Path.GetFullPath(backupPath))
                                     .Where(
                                         f =>
                                         ".ravendb-full-dump".Equals(Path.GetExtension(f),
                                                                     StringComparison.InvariantCultureIgnoreCase))
                                     .OrderBy(File.GetLastWriteTimeUtc).ToList();
                AssertUsersCountInBackup(1, files.First());
                AssertUsersCountInBackup(2, files.Last());
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

        private void WaitForNextFullBackup(IDocumentStore store)
        {
            var lastFullBackup = DateTime.MinValue;

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
            }, 5000);
        }

        private void AssertUsersCountInBackup(int expectedNumberOfUsers, string file)
        {
            using (var store = NewDocumentStore())
            {
                var smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions(),
                    new DatabaseSmugglerFileSource(file),
                    new DatabaseSmugglerEmbeddedDestination(store.SystemDatabase));

                smuggler.Execute();

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

                WaitForPeriodicExport(store.SystemDatabase, backupStatus, PeriodicExportStatus.PeriodicExportStatusEtags.LastDocsEtag);

                store.DatabaseCommands.Delete(userId, null);

                WaitForPeriodicExport(store.SystemDatabase, backupStatus, PeriodicExportStatus.PeriodicExportStatusEtags.LastDocsDeletionEtag);

            }

            using (var store = NewRemoteDocumentStore())
            {
                var smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions(),
                    new DatabaseSmugglerFileSource(backupPath),
                    new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = store.Url
                    }));

                smuggler.Execute();

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<User>(1));
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
                var smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions(),
                    new DatabaseSmugglerFileSource(backupPath),
                    new DatabaseSmugglerEmbeddedDestination(store.SystemDatabase));

                smuggler.Execute();

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<User>(1));
                }
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

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor => Assert.Equal(1, accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, Etag.Empty, null, 10).Count()));


                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "oren" };
                    session.Store(user);
                    userId = user.Id;
                    session.SaveChanges();
                }

                var documentEtagAfterFirstDelete = Etag.Empty;
                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor => documentEtagAfterFirstDelete = accessor.Staleness.GetMostRecentDocumentEtag());

                using (var session = store.OpenSession())
                {
                    session.Delete(userId);
                    session.SaveChanges();
                }

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(2, accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, Etag.Empty, null, 10).Count()));


                var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null,
                                                                    servers[0].SystemDatabase.ServerUrl +
                                                                    "admin/periodicExport/purge-tombstones?docEtag=" + documentEtagAfterFirstDelete,
                                                                    HttpMethods.Post,
                                                                    new OperationCredentials(null, CredentialCache.DefaultCredentials),
                                                                    store.Conventions);

                store.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ReadResponseJson();

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(1, accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, Etag.Empty, null, 10).Count()));

            }
        }

        [Fact]
        public void PeriodicBackupDoesntProduceExcessiveFilesAndCleanupTombstonesProperly()
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

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.Store(new User { Name = "ayende" });
                    session.SaveChanges();
                }

                WaitForPeriodicExport(store.SystemDatabase, backupStatus);

                // status + one export
                VerifyFilesCount(1 + 1, backupPath);

                store.DatabaseCommands.Delete("users/1", null);
                store.DatabaseCommands.Delete("users/2", null);

                store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                {
                    Assert.Equal(2,
                                 accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, Etag.Empty, null, 20)
                                         .Count());
                });


                WaitForPeriodicExport(store.SystemDatabase, backupStatus);

                // status + two exports
                VerifyFilesCount(1 + 2, backupPath);

                store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                {
                    Assert.Equal(1,
                                 accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, Etag.Empty, null, 20)
                                         .Count());
                });

            }

            IOExtensions.DeleteDirectory(backupPath);
        }

        private void VerifyFilesCount(int expectedFiles, string backupPath)
        {
            Assert.Equal(expectedFiles, Directory.GetFiles(backupPath).Count());
        }

        /// <summary>
        ///  In 2.5 we didn't support deleted documents, so those props aren't available. 
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
                    }.WriteTo(new JsonTextWriter(streamWriter));
                streamWriter.Flush();
            }

            var fileDestination = new DatabaseSmugglerFileDestination(backupPath);
            var state = fileDestination.ReadLastEtagsFromFile(etagFileLocation);

            Assert.Equal("00000000-0000-0000-0000-000000000001", state.LastDocsEtag.ToString());
            Assert.Equal(Etag.Empty, state.LastDocDeleteEtag);
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

                using (var stream = new MemoryStream())
                {
                    var startEtag = store.SystemDatabase.Statistics.LastDocEtag.IncrementBy(-5);
                    var endEtag = startEtag.IncrementBy(2);

                    var options = new DatabaseSmugglerOptions();
                    var notifications = new DatabaseSmugglerNotifications();

                    var state = new DatabaseSmugglerOperationState { LastDocsEtag = startEtag };

                    using (var source = new DatabaseSmugglerEmbeddedSource(store.SystemDatabase))
                    using (var destination = new DatabaseSmugglerStreamDestination(stream))
                    {
                        await source.InitializeAsync(options, CancellationToken.None);
                        await destination.InitializeAsync(options, notifications, CancellationToken.None);

                        var documentSmuggler = new DocumentSmuggler(
                            options,
                            notifications,
                            source,
                            destination, new DatabaseLastEtagsInfo { LastDocsEtag = endEtag });

                        await documentSmuggler.SmuggleAsync(state, CancellationToken.None);
                    }

                    stream.Position = 0;

                    using (var gZipStream = new GZipStream(stream, CompressionMode.Decompress, leaveOpen: true))
                    using (var reader = new StreamReader(gZipStream))
                    {
                        var json = await reader.ReadToEndAsync();

                        // read exported content
                        var exportedDocs = RavenJObject.Parse(json).Value<RavenJArray>("Docs");
                        Assert.Equal(2, exportedDocs.Count());

                        Assert.Equal("01000000-0000-0001-0000-000000000007", exportedDocs.First().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                        Assert.Equal("01000000-0000-0001-0000-000000000008", exportedDocs.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                        Assert.Equal("01000000-0000-0001-0000-000000000008", state.LastDocsEtag.ToString());
                    }
                }

                using (var stream = new MemoryStream())
                {
                    var startEtag = store.SystemDatabase.Statistics.LastDocEtag.IncrementBy(-5);
                    Etag endEtag = null;

                    var options = new DatabaseSmugglerOptions();
                    var notifications = new DatabaseSmugglerNotifications();

                    var state = new DatabaseSmugglerOperationState { LastDocsEtag = startEtag };

                    using (var source = new DatabaseSmugglerEmbeddedSource(store.SystemDatabase))
                    using (var destination = new DatabaseSmugglerStreamDestination(stream))
                    {
                        await source.InitializeAsync(options, CancellationToken.None);
                        await destination.InitializeAsync(options, notifications, CancellationToken.None);

                        var documentSmuggler = new DocumentSmuggler(
                            options,
                            notifications,
                            source,
                            destination, new DatabaseLastEtagsInfo { LastDocsEtag = endEtag });

                        await documentSmuggler.SmuggleAsync(state, CancellationToken.None);
                    }

                    stream.Position = 0;

                    using (var gZipStream = new GZipStream(stream, CompressionMode.Decompress, leaveOpen: true))
                    using (var reader = new StreamReader(gZipStream))
                    {
                        var json = await reader.ReadToEndAsync();

                        // read exported content
                        var exportedDocs = RavenJObject.Parse(json).Value<RavenJArray>("Docs");
                        Assert.Equal(5, exportedDocs.Count());

                        Assert.Equal("01000000-0000-0001-0000-000000000007", exportedDocs.First().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                        Assert.Equal("01000000-0000-0001-0000-00000000000B", exportedDocs.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                        Assert.Equal("01000000-0000-0001-0000-00000000000B", state.LastDocsEtag.ToString());
                    }
                }

                WaitForIndexing(store);

                store.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery()
                {
                    Query = "Tag:Users"
                }).WaitForCompletion();


                Etag user6DeletionEtag = null, user9DeletionEtag = null;

                store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                {
                    user6DeletionEtag = accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, "users/6").Etag;
                    user9DeletionEtag = accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, "users/9").Etag;

                });

                using (var stream = new MemoryStream())
                {
                    var options = new DatabaseSmugglerOptions();
                    var notifications = new DatabaseSmugglerNotifications();

                    var state = new DatabaseSmugglerOperationState { LastDocDeleteEtag = user6DeletionEtag };

                    using (var source = new DatabaseSmugglerEmbeddedSource(store.SystemDatabase))
                    using (var destination = new DatabaseSmugglerStreamDestination(stream))
                    {
                        await source.InitializeAsync(options, CancellationToken.None);
                        await destination.InitializeAsync(options, notifications, CancellationToken.None);

                        var documentDeletionsSmuggler = new DocumentDeletionsSmuggler(
                            options,
                            notifications,
                            source,
                            destination, new DatabaseLastEtagsInfo { LastDocDeleteEtag = user9DeletionEtag });

                        await documentDeletionsSmuggler.SmuggleAsync(state, CancellationToken.None);
                    }

                    stream.Position = 0;

                    using (var gZipStream = new GZipStream(stream, CompressionMode.Decompress, leaveOpen: true))
                    using (var reader = new StreamReader(gZipStream))
                    {
                        var json = await reader.ReadToEndAsync();

                        // read exported content
                        var exportJson = RavenJObject.Parse(json);
                        var docsKeys = exportJson.Value<RavenJArray>("DocsDeletions")
                            .Select(x => x.Value<string>("Key"))
                            .ToArray();

                        Assert.Equal(new[] { "users/7", "users/8", "users/9" }, docsKeys);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task MaxNumberOfItemsToProcessInSingleBatchShouldBeRespectedByDataDumper()
        {
            var path = Path.Combine(NewDataPath(forceCreateDir: true), "raven.dump");

            using (var server = GetNewServer(configureConfig: configuration => configuration.Core.MaxNumberOfItemsToProcessInSingleBatch = 1234))
            {
                var smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions
                    {
                        BatchSize = 4321
                    },
                    new DatabaseSmugglerEmbeddedSource(server.SystemDatabase),
                    new DatabaseSmugglerFileDestination(path));

                Assert.Equal(4321, smuggler._options.BatchSize);

                await smuggler.ExecuteAsync();

                Assert.Equal(1234, smuggler._options.BatchSize);

                smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions
                    {
                        BatchSize = 4321
                    },
                    new DatabaseSmugglerFileSource(path),
                    new DatabaseSmugglerEmbeddedDestination(server.SystemDatabase));

                Assert.Equal(4321, smuggler._options.BatchSize);

                await smuggler.ExecuteAsync();

                Assert.Equal(1234, smuggler._options.BatchSize);

                smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions
                    {
                        BatchSize = 1000
                    },
                    new DatabaseSmugglerEmbeddedSource(server.SystemDatabase),
                    new DatabaseSmugglerFileDestination(path));

                Assert.Equal(1000, smuggler._options.BatchSize);

                await smuggler.ExecuteAsync();

                Assert.Equal(1000, smuggler._options.BatchSize);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task MaxNumberOfItemsToProcessInSingleBatchShouldBeRespectedBySmuggler()
        {
            var path = Path.Combine(NewDataPath(forceCreateDir: true), "raven.dump");

            using (var server = GetNewServer(configureConfig: configuration => configuration.Core.MaxNumberOfItemsToProcessInSingleBatch = 1234))
            {
                var smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions
                    {
                        BatchSize = 4321
                    },
                    new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions { Url = server.Configuration.ServerUrl }),
                    new DatabaseSmugglerFileDestination(path));

                Assert.Equal(4321, smuggler._options.BatchSize);

                await smuggler.ExecuteAsync();

                Assert.Equal(1234, smuggler._options.BatchSize);

                smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions
                    {
                        BatchSize = 4321
                    },
                    new DatabaseSmugglerFileSource(path),
                    new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions { Url = server.Configuration.ServerUrl }));

                Assert.Equal(4321, smuggler._options.BatchSize);

                await smuggler.ExecuteAsync();

                Assert.Equal(1234, smuggler._options.BatchSize);

                smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions
                    {
                        BatchSize = 1000
                    },
                    new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions { Url = server.Configuration.ServerUrl }),
                    new DatabaseSmugglerFileDestination(path));

                Assert.Equal(1000, smuggler._options.BatchSize);

                await smuggler.ExecuteAsync();

                Assert.Equal(1000, smuggler._options.BatchSize);
            }
        }

        private class BigDocument
        {
            private static Random generator = new Random();
            public string Id { get; set; }

            public byte[] Payload { get; set; }

            public BigDocument(int payloadSize)
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

            using (var store = NewRemoteDocumentStore(dataDirectory: dataDir))
            {
                store.Conventions.MaxNumberOfRequestsPerSession = 1000;

                // Prepare everything.
                int serverPort = new Uri(store.Url).Port;
                var server = GetServer(serverPort);
                var outputDirectory = Path.Combine(server.Configuration.Core.DataDirectory, "Export");

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
                var smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions(),
                    new DatabaseSmugglerRemoteSource(
                        new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = "http://localhost:" + serverPort,
                            Database = store.DefaultDatabase
                        }),
                    new DatabaseSmugglerFileDestination(
                        outputDirectory,
                        new DatabaseSmugglerFileDestinationOptions
                        {
                            Incremental = true
                        }));

                var exportResult = await smuggler.ExecuteAsync();

                Assert.NotNull(exportResult);
                Assert.True(!string.IsNullOrWhiteSpace(exportResult.FilePath));

                // Import the first batch
                var importSmuggler = new DatabaseSmuggler(
                     new DatabaseSmugglerOptions
                     {
                         BatchSize = 1
                     },
                     new DatabaseSmugglerFileSource(outputDirectory),
                     new DatabaseSmugglerRemoteDestination(
                         new DatabaseSmugglerRemoteConnectionOptions
                         {
                             Url = "http://localhost:" + serverPort,
                             Database = newDatabase
                         },
                         new DatabaseSmugglerRemoteDestinationOptions
                         {
                             ContinuationToken = continuationToken
                         }));

                await importSmuggler.ExecuteAsync();

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
                exportResult = await smuggler.ExecuteAsync();

                importSmuggler.Notifications.OnDocumentRead += (sender, key) => Console.WriteLine("Read document: " + key);
                importSmuggler.Notifications.OnDocumentWrite += (sender, key) => Console.WriteLine("Wrote document: " + key);

                // Importing the second batch effectively skipping the batch already imported.
                await importSmuggler.ExecuteAsync();

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
    }
}
