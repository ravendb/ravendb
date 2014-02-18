// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1537.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Embedded;
using Raven.Database;
using Raven.Database.Extensions;
using Raven.Database.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
using Xunit;
using System.Linq;
using JsonTextWriter = Raven.Imports.Newtonsoft.Json.JsonTextWriter;

namespace Raven.Tests.Issues
{
    public class RavenDB_1537 : RavenTest
    {

        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "PeriodicBackup";
        }
        public class User
        {
            public string Name { get; set; }
            public string Id { get; set; }
        }

        [Fact]
        public void CanFullBackupToDirectory()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "oren" });
                        var periodicBackupSetup = new PeriodicBackupSetup
                        {
                            LocalFolderName = backupPath,
                            FullBackupIntervalMilliseconds = 500
                        };
                        session.Store(periodicBackupSetup, PeriodicBackupSetup.RavenDocumentKey);

                        session.SaveChanges();
                    }

                    WaitForNextFullBackup(store);
                }
                using (var store = NewDocumentStore())
                {
                    var dataDumper = new DataDumper(store.DocumentDatabase);
                    dataDumper.ImportData(new SmugglerImportOptions
                    {
                        FromFile = Directory.GetFiles(Path.GetFullPath(backupPath))
                          .Where(file => ".ravendb-full-dump".Equals(Path.GetExtension(file), StringComparison.InvariantCultureIgnoreCase))
                          .OrderBy(File.GetLastWriteTimeUtc).First()

                    }, new SmugglerOptions
                    {
                        Incremental = false
                    }).Wait();

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

        [Fact]
        public void CanFullBackupToDirectory_MultipleBackups()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                using (var store = NewDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "oren" });
                        var periodicBackupSetup = new PeriodicBackupSetup
                        {
                            LocalFolderName = backupPath,
                            FullBackupIntervalMilliseconds = 500
                        };
                        session.Store(periodicBackupSetup, PeriodicBackupSetup.RavenDocumentKey);

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
                    store.DatabaseCommands.Get(PeriodicBackupStatus.RavenDocumentKey);
            if (jsonDocument == null)
                return null;
            var periodicBackupStatus = jsonDocument.DataAsJson.JsonDeserialization<PeriodicBackupStatus>();
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
                var dataDumper = new DataDumper(store.DocumentDatabase);

                dataDumper.ImportData(new SmugglerImportOptions
                {
                    FromFile = file

                }, new SmugglerOptions
                {
                    Incremental = false
                }).Wait();

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(expectedNumberOfUsers, session.Query<User>().Count());
                }
            }
        }

        [Fact]
        public void SmugglerCanUnderstandPeriodicBackupFormat()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                string userId;
                using (var session = store.OpenSession())
                {
                    var periodicBackupSetup = new PeriodicBackupSetup
                    {
                        LocalFolderName = backupPath,
                        IntervalMilliseconds = 100
                    };
                    session.Store(periodicBackupSetup, PeriodicBackupSetup.RavenDocumentKey);

                    session.SaveChanges();
                }

                var backupStatus = GetPerodicBackupStatus(store.DocumentDatabase);

                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "oren" };
                    session.Store(user);
                    userId = user.Id;
                    session.SaveChanges();
                }

                WaitForPeriodicBackup(store.DocumentDatabase, backupStatus);

                store.DatabaseCommands.Delete(userId, null);

                WaitForPeriodicBackup(store.DocumentDatabase, backupStatus);

            }

            using (var store = NewRemoteDocumentStore())
            {
                var dataDumper = new SmugglerApi(new RavenConnectionStringOptions
                {
                    Url = store.Url
                });
                dataDumper.ImportData(new SmugglerImportOptions
                {
                    FromFile = backupPath,
                }, new SmugglerOptions
                {
                    Incremental = true,
                }).Wait();

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<User>(1));
                }
            }

            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public void CanBackupDeletion()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                string userId;
                using (var session = store.OpenSession())
                {
                    var periodicBackupSetup = new PeriodicBackupSetup
                    {
                        LocalFolderName = backupPath,
                        IntervalMilliseconds = 100
                    };
                    session.Store(periodicBackupSetup, PeriodicBackupSetup.RavenDocumentKey);

                    session.SaveChanges();
                }

                var backupStatus = GetPerodicBackupStatus(store.DocumentDatabase);

                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "oren" };
                    session.Store(user);
                    userId = user.Id;
                    session.SaveChanges();
                }

                WaitForPeriodicBackup(store.DocumentDatabase, backupStatus);

                store.DatabaseCommands.Delete(userId, null);

                WaitForPeriodicBackup(store.DocumentDatabase, backupStatus);

            }

            using (var store = NewDocumentStore())
            {
                var dataDumper = new DataDumper(store.DocumentDatabase);
                dataDumper.ImportData(new SmugglerImportOptions
                {
                    FromFile = backupPath,
                }, new SmugglerOptions
                {
                    Incremental = true,
                }).Wait();

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<User>(1));
                }
            }

            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public void ShouldDeleteTombStoneAfterNextPut()
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

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, userId);
                    Assert.NotNull(tombstone);
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "ayende" }, userId);
                    session.SaveChanges();
                }

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, userId);
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

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(1, accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, Etag.Empty, null, 10).Count()));

                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "oren" };
                    session.Store(user);
                    userId = user.Id;
                    session.SaveChanges();
                }

                var etagAfterFirstDelete = Etag.Empty;
                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor => etagAfterFirstDelete = accessor.Staleness.GetMostRecentDocumentEtag());

                using (var session = store.OpenSession())
                {
                    session.Delete(userId);
                    session.SaveChanges();
                }

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(2, accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, Etag.Empty, null, 10).Count()));

                var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null,
                                                                    servers[0].SystemDatabase.ServerUrl +
                                                                    "admin/periodicBackup/purge-tombstones?docEtag=" + etagAfterFirstDelete,
                                                                    "POST",
                                                                    new OperationCredentials(null, CredentialCache.DefaultCredentials),
                                                                    store.Conventions);

                store.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ReadResponseJson();

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(1, accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, Etag.Empty, null, 10).Count()));

            }
        }

        [Fact]
        public void PeriodicBackupDoesntProduceExcessiveFilesAndCleanupTombstonesProperly()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                string userId;
                using (var session = store.OpenSession())
                {
                    var periodicBackupSetup = new PeriodicBackupSetup
                    {
                        LocalFolderName = backupPath,
                        IntervalMilliseconds = 10000
                    };
                    session.Store(periodicBackupSetup, PeriodicBackupSetup.RavenDocumentKey);

                    session.SaveChanges();
                }

                var backupStatus = GetPerodicBackupStatus(store.DocumentDatabase);

                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "oren" };
                    session.Store(user);
                    userId = user.Id;
                    session.SaveChanges();
                }

                WaitForPeriodicBackup(store.DocumentDatabase, backupStatus);

                // status + one export
                VerifyFilesCount(1 + 1, backupPath);

                store.DatabaseCommands.Delete(userId, null);

                WaitForPeriodicBackup(store.DocumentDatabase, backupStatus);

                // status + two exports
                VerifyFilesCount(1 + 2, backupPath);

            }

            IOExtensions.DeleteDirectory(backupPath);
        }

        private void VerifyFilesCount(int expectedFiles, string backupPath)
        {
            Assert.Equal(expectedFiles, Directory.GetFiles(backupPath).Count());
        }

        //TODO: tests for documents and attachments
        //TODO: test back with only deletion!

        /// <summary>
        ///  In 2.5 we didn't support deleted documents/attachments, so those props aren't available. 
        /// </summary>
        [Fact]
        public void CanProperlyReadLastEtagUsingPreviousFormat()
        {
            var backupPath = NewDataPath("BackupFolder");

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

            var result = new ExportDataResult
            {
                FilePath = backupPath
            };
            SmugglerApiBase.ReadLastEtagsFromFile(result);

            Assert.Equal("00000000-0000-0000-0000-000000000001", result.LastDocsEtag.ToString());
            Assert.Equal("00000000-0000-0000-0000-000000000002", result.LastAttachmentsEtag.ToString());
            Assert.Equal(Etag.Empty, result.LastDocDeleteEtag);
            Assert.Equal(Etag.Empty, result.LastAttachmentsDeleteEtag);
        }

        public class CustomDataDumper : DataDumper
        {
            public CustomDataDumper(DocumentDatabase database)
                : base(database)
            {
            }

            public new Task<Etag> ExportDocuments(SmugglerOptions options, JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
            {
                return base.ExportDocuments(options, jsonWriter, lastEtag, maxEtag);
            }

            public new Task<Etag> ExportAttachments(JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
            {
                return base.ExportAttachments(jsonWriter, lastEtag, maxEtag);
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
                        session.Store(new User { Name = "oren #" + i });
                    }
                    session.SaveChanges();
                }

                using (var textStream = new StringWriter())
                using (var writer = new JsonTextWriter(textStream))
                {
                    var dumper = new CustomDataDumper(store.DocumentDatabase)
                    {
                        SmugglerOptions = new SmugglerOptions()
                    };

                    var startEtag = store.DocumentDatabase.Statistics.LastDocEtag.IncrementBy(-5);
                    var endEtag = startEtag.IncrementBy(2);

                    writer.WriteStartArray();
                    var lastEtag = await dumper.ExportDocuments(new SmugglerOptions(), writer, startEtag, endEtag);
                    writer.WriteEndArray();
                    writer.Flush();

                    // read exported content
                    var exportedDocs = RavenJArray.Parse(textStream.GetStringBuilder().ToString());
                    Assert.Equal(2, exportedDocs.Count());

                    Assert.Equal("01000000-0000-0001-0000-000000000007", exportedDocs.First().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                    Assert.Equal("01000000-0000-0001-0000-000000000008", exportedDocs.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                    Assert.Equal("01000000-0000-0001-0000-000000000008", lastEtag.ToString());

                }

                using (var textStream = new StringWriter())
                using (var writer = new JsonTextWriter(textStream))
                {
                    var dumper = new CustomDataDumper(store.DocumentDatabase)
                    {
                        SmugglerOptions = new SmugglerOptions()
                    };

                    var startEtag = store.DocumentDatabase.Statistics.LastDocEtag.IncrementBy(-5);

                    writer.WriteStartArray();
                    var lastEtag = await dumper.ExportDocuments(new SmugglerOptions(), writer, startEtag, null);
                    writer.WriteEndArray();
                    writer.Flush();

                    // read exported content
                    var exportedDocs = RavenJArray.Parse(textStream.GetStringBuilder().ToString());
                    Assert.Equal(5, exportedDocs.Count());

                    Assert.Equal("01000000-0000-0001-0000-000000000007", exportedDocs.First().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                    Assert.Equal("01000000-0000-0001-0000-00000000000B", exportedDocs.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
                    Assert.Equal("01000000-0000-0001-0000-00000000000B", lastEtag.ToString());
                }

                for (var i = 0; i < 10; i++)
                {
                    store.DatabaseCommands.PutAttachment("attach/" + (i+1), null, new MemoryStream(new [] { (byte)i }), new RavenJObject());
                }

                using (var textStream = new StringWriter())
                using (var writer = new JsonTextWriter(textStream))
                {
                    var dumper = new CustomDataDumper(store.DocumentDatabase)
                    {
                        SmugglerOptions = new SmugglerOptions()
                    };

                    var startEtag = store.DocumentDatabase.Statistics.LastAttachmentEtag.IncrementBy(-5);
                    var endEtag = startEtag.IncrementBy(2);

                    writer.WriteStartArray();
                    var lastEtag = await dumper.ExportAttachments(writer, startEtag, endEtag);
                    writer.WriteEndArray();
                    writer.Flush();

                    // read exported content
                    var exportedAttachments = RavenJArray.Parse(textStream.GetStringBuilder().ToString());
                    Assert.Equal(2, exportedAttachments.Count());

                    Assert.Equal("02000000-0000-0001-0000-000000000006", exportedAttachments.First().Value<string>("Etag"));
                    Assert.Equal("02000000-0000-0001-0000-000000000007", exportedAttachments.Last().Value<string>("Etag"));
                    Assert.Equal("02000000-0000-0001-0000-000000000007", lastEtag.ToString());

                }

                using (var textStream = new StringWriter())
                using (var writer = new JsonTextWriter(textStream))
                {
                    var dumper = new CustomDataDumper(store.DocumentDatabase)
                    {
                        SmugglerOptions = new SmugglerOptions()
                    };

                    var startEtag = store.DocumentDatabase.Statistics.LastAttachmentEtag.IncrementBy(-5);

                    writer.WriteStartArray();
                    var lastEtag = await dumper.ExportAttachments(writer, startEtag, null);
                    writer.WriteEndArray();
                    writer.Flush();

                    // read exported content
                    var exportedAttachments = RavenJArray.Parse(textStream.GetStringBuilder().ToString());
                    Assert.Equal(5, exportedAttachments.Count());

                    Assert.Equal("02000000-0000-0001-0000-000000000006", exportedAttachments.First().Value<string>("Etag"));
                    Assert.Equal("02000000-0000-0001-0000-00000000000A", exportedAttachments.Last().Value<string>("Etag"));
                    Assert.Equal("02000000-0000-0001-0000-00000000000A", lastEtag.ToString());

                }

                //TODO: finish for document deletions + attachment deletions




            }
        }
    }
}