// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1537.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
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
using Raven.Tests.Common;

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
            var backupPath = NewDataPath("BackupFolder", forceCreateDir: true);
            try
            {
                using (var store = NewDocumentStore())
                {
                    store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1,2,3,4,5 }), new RavenJObject());

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
                        Assert.NotNull(store.DatabaseCommands.GetAttachment("attach/1"));
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
            var backupPath = NewDataPath("BackupFolder", forceCreateDir:true);
            try
            {
                using (var store = NewDocumentStore())
                {

                    store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] {1,2,3,4,5}), new RavenJObject());
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "oren" });
                        var periodicBackupSetup = new PeriodicBackupSetup
                        {
                            LocalFolderName = backupPath,
                            FullBackupIntervalMilliseconds = 250
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
                store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1,2,3,4 }), new RavenJObject());

                WaitForPeriodicBackup(store.DocumentDatabase, backupStatus);

                store.DatabaseCommands.Delete(userId, null);
                store.DatabaseCommands.DeleteAttachment("attach/1", null);

                WaitForPeriodicBackup(store.DocumentDatabase, backupStatus);

            }

            using (var store = NewRemoteDocumentStore())
            {
	            var connection = new RavenConnectionStringOptions
	            {
		            Url = store.Url
	            };
	            var dataDumper = new SmugglerApi();
                dataDumper.ImportData(new SmugglerImportOptions
                {
                    FromFile = backupPath,
					To = connection,
                }, new SmugglerOptions
                {
                    Incremental = true,
                }).Wait();

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<User>(1));
                    Assert.Null(store.DatabaseCommands.GetAttachment("attach/1"));
                }
            }

            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public void CanBackupDocumentDeletion()
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
        public void CanBackupAttachmentDeletion()
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
                        IntervalMilliseconds = 250 
                    };
                    session.Store(periodicBackupSetup, PeriodicBackupSetup.RavenDocumentKey);

                    session.SaveChanges();
                }

                var backupStatus = GetPerodicBackupStatus(store.DocumentDatabase);

                store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1,2,3,4}), new RavenJObject());

                WaitForPeriodicBackup(store.DocumentDatabase, backupStatus);

                store.DatabaseCommands.DeleteAttachment("attach/1", null);

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
        public void ShouldDeleteAttachmentTombStoneAfterNextPut()
        {
            using (EmbeddableDocumentStore store = NewDocumentStore())
            {
                // create document
                store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1,2,3,4,5}), new RavenJObject());

                //now delete it and check for tombstone
                store.DatabaseCommands.DeleteAttachment("attach/1", null);

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, "attach/1");
                    Assert.NotNull(tombstone);
                });

                store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }), new RavenJObject());

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    var tombstone = accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, "attach/1");
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
                                                                                          .RavenPeriodicBackupsDocsTombstones,
                                                                                      Etag.Empty, null, 10).Count()));

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(1, accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, Etag.Empty, null, 10).Count()));

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
                    Assert.Equal(2, accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, Etag.Empty, null, 10).Count()));

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(2, accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, Etag.Empty, null, 10).Count()));

                var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null,
                                                                    servers[0].SystemDatabase.ServerUrl +
                                                                    "admin/periodicBackup/purge-tombstones?docEtag=" + documentEtagAfterFirstDelete + "&attachmentEtag=" + attachmentEtagAfterFirstDelete,
                                                                    "POST",
                                                                    new OperationCredentials(null, CredentialCache.DefaultCredentials),
                                                                    store.Conventions);

                store.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ReadResponseJson();

                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(1, accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, Etag.Empty, null, 10).Count()));
                servers[0].SystemDatabase.TransactionalStorage.Batch(accessor =>
                    Assert.Equal(1, accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, Etag.Empty, null, 10).Count()));

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
                        IntervalMilliseconds = 250
                    };
                    session.Store(periodicBackupSetup, PeriodicBackupSetup.RavenDocumentKey);

                    session.SaveChanges();
                }

                var backupStatus = GetPerodicBackupStatus(store.DocumentDatabase);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren"});
                    session.Store(new User { Name = "ayende"});
                    store.DatabaseCommands.PutAttachment("attach/1", null, new MemoryStream(new byte[] { 1,2,3,4,5}), new RavenJObject());
                    store.DatabaseCommands.PutAttachment("attach/2", null, new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }), new RavenJObject());
                    session.SaveChanges();
                }

                WaitForPeriodicBackup(store.DocumentDatabase, backupStatus);

                // status + one export
                VerifyFilesCount(1 + 1, backupPath);

                store.DatabaseCommands.Delete("users/1", null);
                store.DatabaseCommands.Delete("users/2", null);
                store.DatabaseCommands.DeleteAttachment("attach/1", null);
                store.DatabaseCommands.DeleteAttachment("attach/2", null);

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    Assert.Equal(2,
                                 accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, Etag.Empty, null, 20)
                                         .Count());
                    Assert.Equal(2,
                                 accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, Etag.Empty, null, 20)
                                         .Count());
                });


                WaitForPeriodicBackup(store.DocumentDatabase, backupStatus);

                // status + two exports
                VerifyFilesCount(1 + 2, backupPath);

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    Assert.Equal(1,
                                 accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, Etag.Empty, null, 20)
                                         .Count());
                    Assert.Equal(1,
                                 accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, Etag.Empty, null, 20)
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
        ///  In 2.5 we didn't support deleted documents/attachments, so those props aren't available. 
        /// </summary>
        [Fact]
        public void CanProperlyReadLastEtagUsingPreviousFormat()
        {
            var backupPath = NewDataPath("BackupFolder", forceCreateDir:true);

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

        /// <summary>
        /// Purpose of this class is to expose few protected method and make them easily testable without using reflection.
        /// </summary>
        public class CustomDataDumper : DataDumper
        {
            public CustomDataDumper(DocumentDatabase database)
                : base(database)
            {
            }

            public new Task<Etag> ExportDocuments(SmugglerOptions options, JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
            {
                return base.ExportDocuments(new RavenConnectionStringOptions(), options, jsonWriter, lastEtag, maxEtag);
            }

            public new Task<Etag> ExportAttachments(JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
            {
                return base.ExportAttachments(new RavenConnectionStringOptions(), jsonWriter, lastEtag, maxEtag);
            }

            public new void ExportDeletions(JsonTextWriter jsonWriter, SmugglerOptions options, ExportDataResult result, LastEtagsInfo maxEtags)
            {
                base.ExportDeletions(jsonWriter, options, result, maxEtags);
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
                        session.Store(new User { Name = "oren #" + (i+1) });
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

                WaitForIndexing(store);

                store.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery()
                {
                    Query = "Tag:Users"
                }).WaitForCompletion();

                for (var i = 0; i < 10; i++)
                {
                    store.DatabaseCommands.DeleteAttachment("attach/" + (i+1), null);
                }

                Etag user6DeletionEtag = null, user9DeletionEtag = null, attach5DeletionEtag = null, attach7DeletionEtag = null;
                
                WaitForUserToContinueTheTest(store);

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    user6DeletionEtag =
                        accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, "users/6").Etag;
                    user9DeletionEtag =
                        accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, "users/9").Etag;
                    attach5DeletionEtag =
                        accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, "attach/5").Etag;
                    attach7DeletionEtag =
                        accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, "attach/7").Etag;

                });

                using (var textStream = new StringWriter())
                using (var writer = new JsonTextWriter(textStream))
                {
                    var dumper = new CustomDataDumper(store.DocumentDatabase)
                    {
                        SmugglerOptions = new SmugglerOptions()
                    };

                    writer.WriteStartObject();
                    var lastEtags = new LastEtagsInfo();
                    var exportResult = new ExportDataResult
                    {
                        LastDocDeleteEtag = user6DeletionEtag,
                        LastAttachmentsDeleteEtag = attach5DeletionEtag
                    };

                    lastEtags.LastDocDeleteEtag = user9DeletionEtag;
                    lastEtags.LastAttachmentsDeleteEtag = attach7DeletionEtag;
                    dumper.ExportDeletions(writer, new SmugglerOptions(), exportResult, lastEtags);
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
                    Assert.Equal(new [] { "users/7", "users/8", "users/9" }, docsKeys);
                    Assert.Equal(new [] { "attach/6", "attach/7" }, attachmentsKeys);
                }
            }
        }
    }
}