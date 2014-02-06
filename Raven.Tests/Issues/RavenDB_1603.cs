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
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Smuggler;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Triggers;
using Xunit;
using Raven.Abstractions.Extensions;
using System.Linq;

namespace Raven.Tests.Issues
{

    public class PortForwarder
    {
        private readonly int listenPort;
        private readonly int targetPort;
        private Func<int, byte[], int, int, bool> shouldThrow;
        private readonly CancellationTokenSource cancellationTokenSource;
        private TcpListener server;

        public PortForwarder(int listenPort, int targetPort, Func<int, byte[], int, int, bool> shouldThrow)
        {
            this.listenPort = listenPort;
            this.targetPort = targetPort;
            this.shouldThrow = shouldThrow;
            cancellationTokenSource = new CancellationTokenSource();
        }

        public void Stop()
        {
            server.Stop();
            cancellationTokenSource.Cancel();
        }

        public void Forward()
        {
            var localAddr = IPAddress.Parse("127.0.0.1");
            server = new TcpListener(localAddr, listenPort);
            server.Start();

            Task.Run(() =>
            {
                var token = cancellationTokenSource.Token;
                while (true)
                {
                    var tcpClient = server.AcceptTcpClient();
                    Task.Run(() => HandleClient(tcpClient));
                }
            });
        }

        private void HandleClient(TcpClient tcpClient)
        {
            var incomingStream = tcpClient.GetStream();

            // now connect to target

            var targetClient = new TcpClient("127.0.0.1", targetPort);
            var targetStream = targetClient.GetStream();

            var readTask = Task.Run(async () =>
            {
                var buffer = new byte[4096];
                var totalRead = 0;
                var token = cancellationTokenSource.Token;
                while (true)
                {
                    var read = await incomingStream.ReadAsync(buffer, 0, 4096, token);
                    if (read == 0)
                    {
                        tcpClient.Close();
                        break;
                    }
                    totalRead += read;
                    if (shouldThrow(totalRead, buffer,0, read))
                    {
                        incomingStream.Close();
                        targetStream.Close();
                    }
                    await targetStream.WriteAsync(buffer, 0, read, token);
                    targetStream.Flush();
                }
            });

            var writeTask = Task.Run(async () =>
            {
                var token = cancellationTokenSource.Token;
                var buffer = new byte[4096];
                var totalRead = 0;
                while (true)
                {
                    var read = await targetStream.ReadAsync(buffer, 0, 4096, token);
                    if (read == 0)
                    {
                        targetClient.Close();
                    }
                    totalRead += read;
                    if (shouldThrow(totalRead, buffer, 0, read))
                    {
                        incomingStream.Close();
                        targetStream.Close();
                    }
                    await incomingStream.WriteAsync(buffer, 0, read, token);
                    incomingStream.Flush();
                }
            });

            Task.WaitAll(readTask, writeTask);
        }
    }

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
                                                                   typeof(ReadTriggers.HiddenDocumentsTrigger)));
        }

        [Fact]
        public async Task CanPerformDump_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                InsertUsers(store, 0, 2000);

                var dumper = CreateDataDumper(store.DocumentDatabase);
                await dumper.ExportData(new SmugglerExportOptions
                                        {
                                            ToFile = backupPath,
                                        }, new SmugglerOptions
                                        {
                                            Incremental = true
                                        });
            }

            VerifyDump(backupPath, store =>
            {
                using (var session = store.OpenSession())
                {
                    Assert.Equal(2000, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                }
            });
            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public async Task CanPerformDump_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewRemoteDocumentStore())
            {
                InsertUsers(store, 0, 2000);

                var dumper = CreateSmuggler("http://localhost:8079", store.DefaultDatabase);

                await dumper.ExportData(new SmugglerExportOptions
                                        {
                                            ToFile = backupPath
                                        }, new SmugglerOptions
                                        {
                                            Incremental = true
                                        });
            }

            VerifyDump(backupPath, store =>
            {
                using (var session = store.OpenSession())
                {
                    Assert.Equal(2000, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                }
            });
            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public async Task CanPerformDumpWithLimit_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                InsertUsers(store, 0, 2000);

                var options = new SmugglerOptions
                              {
                                  Limit = 1500,
                                  Incremental = true,
                                  Filters =
                                  {
                                      new FilterSetting
                                      {
                                          Path = "@metadata.Raven-Entity-Name",
                                          Values = { "Users" },
                                          ShouldMatch = true,
                                      }
                                  }
                              };

                var dumper = CreateDataDumper(store.DocumentDatabase);
                await dumper.ExportData(new SmugglerExportOptions
                                        {
                                            ToFile = backupPath
                                        }, options);
            }


            VerifyDump(backupPath, store =>
            {
                using (var session = store.OpenSession())
                {
                    Assert.Equal(1500, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                }
            });
            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public async Task CanPerformDumpWithLimit_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewRemoteDocumentStore())
            {
                InsertUsers(store, 0, 2000);

                var options = new SmugglerOptions
                              {
                                  Limit = 1500,
                                  Incremental = true,
                                  Filters =
                                  {
                                      new FilterSetting
                                      {
                                          Path = "@metadata.Raven-Entity-Name",
                                          Values = { "Users" },
                                          ShouldMatch = true,
                                      }
                                  }
                              };

                var dumper = CreateSmuggler("http://localhost:8079", store.DefaultDatabase);
                await dumper.ExportData(new SmugglerExportOptions
                                        {
                                            ToFile = backupPath
                                        }, options);
            }

            VerifyDump(backupPath, store =>
            {
                using (var session = store.OpenSession())
                {
                    Assert.Equal(1500, session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                }
            });
            IOExtensions.DeleteDirectory(backupPath);
        }

        private void VerifyDump(string backupPath, Action<EmbeddableDocumentStore> action)
        {
            using (var store = NewDocumentStore())
            {
                var dumper = CreateDataDumper(store.DocumentDatabase);
                dumper.ImportData(new SmugglerImportOptions
                                      {
                                          FromFile = backupPath
                                      }, new SmugglerOptions
                                      {
                                          Incremental = true
                                      }).Wait();

                action(store);
            }
        }

        [Fact]
        public async Task CanPerformDumpWithLimitAndFilter_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                var counter = 0;
                counter = InsertUsers(store, counter, 1000);
                counter = InsertDevelopers(store, counter, 2);
                counter = InsertUsers(store, counter, 1000);
                InsertDevelopers(store, counter, 2);

                WaitForIndexing(store);

                var options = new SmugglerOptions
                {
                    Limit = 5,
                    Incremental = true,
                    Filters =
                {
                    new FilterSetting
                    {
                        Path = "@metadata.Raven-Entity-Name",
                        Values = {"Developers"},
                        ShouldMatch = true,
                    }
                }
                };
                var dumper = CreateDataDumper(store.DocumentDatabase);
                await dumper.ExportData(new SmugglerExportOptions
                                        {
                                            ToFile = backupPath
                                        }, options);

            }


            VerifyDump(backupPath, store =>
            {
                using (var session = store.OpenSession())
                {
                    Assert.Equal(4, session.Query<Developer>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                }
            });

            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public async Task CanPerformDumpWithLimitAndFilter_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewRemoteDocumentStore())
            {
                var counter = 0;
                counter = InsertUsers(store, counter, 1000);
                counter = InsertDevelopers(store, counter, 2);
                counter = InsertUsers(store, counter, 1000);
                InsertDevelopers(store, counter, 2);

                WaitForIndexing(store);

                var options = new SmugglerOptions
                              {
                                  Limit = 5,
                                  Incremental = true,
                                  Filters =
                                  {
                                      new FilterSetting
                                      {
                                          Path = "@metadata.Raven-Entity-Name",
                                          Values = { "Developers" },
                                          ShouldMatch = true,
                                      }
                                  }
                              };

                var dumper = CreateSmuggler("http://localhost:8079", store.DefaultDatabase);
                await dumper.ExportData(new SmugglerExportOptions
                                        {
                                            ToFile = backupPath,
                                        }, options);
            }


            VerifyDump(backupPath, store =>
            {
                using (var session = store.OpenSession())
                {
                    Assert.Equal(4, session.Query<Developer>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Count());
                }
            });

            IOExtensions.DeleteDirectory(backupPath);
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
                        session.Store(new User { Name = "User #" + counter });
                    }
                    session.SaveChanges();
                }
            }
            return counter;
        }

        [Fact]
        public async Task CanDumpWhenHiddenDocs_Dumper()
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
                    var dumper = CreateDataDumper(server.SystemDatabase);

                    await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true });
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

        [Fact]
        public async Task CanDumpWhenHiddenDocs_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (NewRemoteDocumentStore())
            {
                using (var store = new DocumentStore { Url = "http://localhost:8079" })
                {
                    store.Initialize();

                    InsertHidenUsers(store, 2000);

                    var user1 = store.DatabaseCommands.Get("users/1");
                    Assert.Null(user1);

                    InsertUsers(store, 1, 25);

                    // now perform full backup
                    var dumper = CreateSmuggler("http://localhost:8079", store.DefaultDatabase);
                    await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true });
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

        [Fact]
        public async Task CanDumpEmptyDatabase_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var server = GetNewServer(databaseName: Constants.SystemDatabase))
            {
                using (new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                {
                    // now perform full backup
                    var dumper = CreateDataDumper(server.SystemDatabase);
                    await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true });
                }
            }

            VerifyDump(backupPath, store => Assert.Equal(0, store.DocumentDatabase.GetDocuments(0, int.MaxValue, null, CancellationToken.None).Count()));

            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public async Task CanDumpEmptyDatabase_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewRemoteDocumentStore())
            {
                // now perform full backup
                var dumper = CreateSmuggler("http://localhost:8079", store.DefaultDatabase);
                await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true });
            }

            VerifyDump(backupPath, store => Assert.Equal(0, store.DocumentDatabase.GetDocuments(0, int.MaxValue, null, CancellationToken.None).Count()));

            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
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
                    var dumper = CreateDataDumper(server.SystemDatabase);
                    await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true });
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

        [Fact]
        public async Task CanDumpWhenHiddenDocsWithLimit_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
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
                    var dumper = CreateSmuggler("http://localhost:8079", store.DefaultDatabase);
                    await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true });
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

        [Fact]
        public async Task CanDumpAttachments_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                InsertAttachments(store, 328);

                var dumper = CreateDataDumper(store.DocumentDatabase);
                await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true, BatchSize = 100});
            }

            VerifyDump(backupPath, store => Assert.Equal(328, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count()));
            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public async Task CanDumpAttachments_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewRemoteDocumentStore())
            {
                InsertAttachments(store, 328);

                var dumper = CreateSmuggler("http://localhost:8079", store.DefaultDatabase);
                await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true, BatchSize = 100 });
            }

            VerifyDump(backupPath, store => Assert.Equal(328, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count()));
            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public async Task CanDumpAttachmentsWithLimit_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                InsertAttachments(store, 328);

                var dumper = CreateDataDumper(store.DocumentDatabase);
                await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true, BatchSize = 100, Limit = 206});
            }

            VerifyDump(backupPath, store => Assert.Equal(206, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count()));
            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public async Task CanDumpAttachmentsWithLimit_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewRemoteDocumentStore())
            {
                InsertAttachments(store, 328);

                var dumper = CreateSmuggler("http://localhost:8079", store.DefaultDatabase);
                await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true, BatchSize = 100, Limit = 206 });
            }

            VerifyDump(backupPath, store => Assert.Equal(206, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count()));
            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public async Task CanDumpAttachmentsEmpty_Dumper()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewDocumentStore())
            {
                var dumper = CreateDataDumper(store.DocumentDatabase);
                await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true, BatchSize = 100, Limit = 206 });
            }

            VerifyDump(backupPath, store =>
            {
                Assert.Equal(0, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count());
            });
            IOExtensions.DeleteDirectory(backupPath);
        }

        [Fact]
        public async Task CanDumpAttachmentsEmpty_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            using (var store = NewRemoteDocumentStore())
            {
                var dumper = CreateSmuggler("http://localhost:8079", store.DefaultDatabase);
                await dumper.ExportData(new SmugglerExportOptions { ToFile = backupPath }, new SmugglerOptions { Incremental = true, BatchSize = 100, Limit = 206 });
            }

            VerifyDump(backupPath, store =>
            {
                Assert.Equal(0, store.DatabaseCommands.GetAttachmentHeadersStartingWith("user", 0, 500).Count());
            });
            IOExtensions.DeleteDirectory(backupPath);
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


        [Fact]
        public async Task CanHandleDocumentExceptionsGracefully_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            var server = GetNewServer(databaseName: Constants.SystemDatabase);

            var alreadyReset = false;

            var forwarder = new PortForwarder(8070, 8079, (totalRead, bytes, offset, count) =>
            {
                if (alreadyReset == false && totalRead > 10000)
                {
                    alreadyReset = true;
                    return true;
                }
                return false;
            });
            forwarder.Forward();
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

                var options = new SmugglerOptions
                {
                    Limit = 1500,
                    Incremental = true
                };

                var dumper = CreateSmuggler("http://localhost:8070", databaseName);

                var allDocs = new List<RavenJObject>();

                ExportDataResult exportResult = null;

                try
                {
                    exportResult = dumper.ExportData(new SmugglerExportOptions
                    {
                        ToFile = backupPath
                    }, options).Result;
                    Assert.False(true, "Previous op should throw.");
                }
                catch (AggregateException e)
                {
                    var inner = (SmugglerExportException)e.ExtractSingleInnerException();
                    exportResult = new ExportDataResult
                    {
                        FilePath = inner.File
                    };
                }

                using (var fileSteam = new FileStream(exportResult.FilePath, FileMode.Open))
                using (var stream = new GZipStream(fileSteam, CompressionMode.Decompress))
                {
                    var chunk1 = RavenJToken.TryLoad(stream) as RavenJObject;
                    var doc1 = chunk1["Docs"] as RavenJArray;
                    allDocs.AddRange(doc1.Values<RavenJObject>());
                }

                exportResult = await dumper.ExportData(new SmugglerExportOptions
                {
                    ToFile = backupPath
                }, options);
                using (var fileStream = new FileStream(exportResult.FilePath, FileMode.Open))
                using (var stream = new GZipStream(fileStream, CompressionMode.Decompress))
                {
                    var chunk2 = RavenJToken.TryLoad(stream) as RavenJObject;
                    var doc2 = chunk2["Docs"] as RavenJArray;
                    allDocs.AddRange(doc2.Values<RavenJObject>());
                }

                Assert.Equal(2000, allDocs.Count(d => (d.Value<string>("Name") ?? String.Empty).StartsWith("User")));
                
                IOExtensions.DeleteDirectory(backupPath);
            }
            finally
            {
                forwarder.Stop();
                server.Dispose();
            }
        }

        [Fact]
        public async Task CanHandleAttachmentExceptionsGracefully_Smuggler()
        {
            var backupPath = NewDataPath("BackupFolder");
            var server = GetNewServer();

            var resetCount = 0;

            var forwarder = new PortForwarder(8070, 8079, (totalRead, bytes, offset, count) =>
            {
                var payload = System.Text.Encoding.UTF8.GetString(bytes, offset, count);
                //reset count is requred as raven can retry attachment download
                if (payload.Contains("GET /static/users/678 ") && resetCount < 5)
                {
                    resetCount++;
                    return true;
                }
                return false;
            });
            forwarder.Forward();
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

                var options = new SmugglerOptions
                {
                    Limit = 1500,
                    Incremental = true
                };
                var dumper = CreateSmuggler("http://localhost:8070", databaseName);

                var allAttachments = new List<RavenJObject>();

                ExportDataResult exportResult = null;
                try
                {
                    exportResult = dumper.ExportData(new SmugglerExportOptions
                    {
                        ToFile = backupPath
                    }, options).Result;
                    Assert.False(true, "Previous op should throw.");
                }
                catch (AggregateException e)
                {
                    var inner = (SmugglerExportException)e.ExtractSingleInnerException();
                    exportResult = new ExportDataResult
                    {
                        FilePath = inner.File
                    };
                }

                using (var fileStream = new FileStream(exportResult.FilePath, FileMode.Open))
                using (var stream = new GZipStream(fileStream, CompressionMode.Decompress))
                {
                    var chunk1 = RavenJToken.TryLoad(stream) as RavenJObject;
                    var att1 = chunk1["Attachments"] as RavenJArray;
                    allAttachments.AddRange(att1.Values<RavenJObject>());
                }

                exportResult = await dumper.ExportData(new SmugglerExportOptions
                                        {
                                            ToFile = backupPath
                                        }, options);
                using (var fileStream = new FileStream(exportResult.FilePath, FileMode.Open))
                using (var stream = new GZipStream(fileStream, CompressionMode.Decompress))
                {
                    var chunk2 = RavenJToken.TryLoad(stream) as RavenJObject;
                    var attr2 = chunk2["Attachments"] as RavenJArray;
                    allAttachments.AddRange(attr2.Values<RavenJObject>());
                }

                Assert.Equal(2000, allAttachments.Count());

                IOExtensions.DeleteDirectory(backupPath);
            }
            finally
            {
                forwarder.Stop();
                server.Dispose();
            }
        }
        
        private DataDumper CreateDataDumper(DocumentDatabase database)
        {
            return new DataDumper(database);
        }

        private SmugglerApi CreateSmuggler(string url, string databaseName)
        {
            return new SmugglerApi(new RavenConnectionStringOptions { Url = url, DefaultDatabase = databaseName });
        }

    }
}