// -----------------------------------------------------------------------
//  <copyright file="IndexRecovery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.IO;
using System.Linq;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Indexing;
using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Indexes.Recovery
{
    using Client.Connection;
    using Imports.Newtonsoft.Json.Linq;
    using Raven.Abstractions.Data;
    using Raven.Abstractions.Extensions;
    using Raven.Json.Linq;

    public class MapIndexRecoveryTests : RavenTest
    {
        private void CommitPointAfterEachCommit(InMemoryRavenConfiguration configuration)
        {
            // force commit point creation after each commit
            configuration.MinIndexingTimeIntervalToStoreCommitPoint = TimeSpan.FromSeconds(0);
            configuration.MaxIndexCommitPointStoreTimeInterval = TimeSpan.FromSeconds(0);
        }

        private void CommitPointAfterFirstCommitOnly(InMemoryRavenConfiguration configuration)
        {
            // by default first commit will force creating commit point, here we don't need more
            configuration.MinIndexingTimeIntervalToStoreCommitPoint = TimeSpan.FromMinutes(30);
            configuration.MaxIndexCommitPointStoreTimeInterval = TimeSpan.MaxValue;
        }

        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.DefaultStorageTypeName = "esent";
            configuration.FlushIndexToDiskSizeInMb = 0;
        }

        [Fact]
        public void ShouldCreateCommitPointsForMapIndexes()
        {
            var index = new MapRecoveryTestIndex();

            string commitPointsDirectory;
            using (var server = GetNewServer(runInMemory: false))
            {
                CommitPointAfterEachCommit(server.SystemDatabase.Configuration);

                using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                {
                    index.Execute(store);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new Recovery
                        {
                            Name = "One",
                            Number = 1
                        });

                        session.SaveChanges();
                        WaitForIndexing(store);

                        session.Store(new Recovery
                        {
                            Name = "Two",
                            Number = 2
                        });

                        session.SaveChanges();
                        WaitForIndexing(store);
                    }

                    Index indexInstance = server.SystemDatabase.IndexStorage.GetIndexInstance(index.IndexName);

                    commitPointsDirectory = Path.Combine(server.SystemDatabase.Configuration.IndexStoragePath,
                                                         indexInstance.IndexId + "\\CommitPoints");
                }

                Assert.True(Directory.Exists(commitPointsDirectory));

                var commitPoints = Directory.GetDirectories(commitPointsDirectory);

                Assert.Equal(2, commitPoints.Length);

                foreach (var commitPoint in commitPoints)
                {
                    var files = Directory.GetFiles(commitPoint);

                    Assert.Equal(2, files.Length);

                    Assert.True(files.Any(file => Path.GetFileName(file) == "index.commitPoint"));
                    Assert.True(files.Any(file => Path.GetFileName(file).StartsWith("segments_")));
                }
            }
        }

        [Fact]
        public void ShouldKeepLimitedNumberOfCommitPoints()
        {
            var index = new MapRecoveryTestIndex();

            using (var server = GetNewServer(runInMemory: false))
            {
                CommitPointAfterEachCommit(server.SystemDatabase.Configuration);

                var maxNumberOfStoredCommitPoints = server.SystemDatabase.Configuration.MaxNumberOfStoredCommitPoints;

                using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                {
                    index.Execute(store);

                    for (int i = 0; i < 2 * maxNumberOfStoredCommitPoints; i++)
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Store(new Recovery
                            {
                                Name = i.ToString(),
                                Number = i
                            });

                            session.SaveChanges();
                            WaitForIndexing(store);
                        }

                        Index indexInstance = server.SystemDatabase.IndexStorage.GetIndexInstance(index.IndexName);

                        var commitPointsDirectory = Path.Combine(server.SystemDatabase.Configuration.IndexStoragePath,
                                                              indexInstance.IndexId + "\\CommitPoints");

                        var commitPoints = Directory.GetDirectories(commitPointsDirectory);

                        Assert.True(commitPoints.Length <= maxNumberOfStoredCommitPoints);
                    }
                }
            }
        }

        [Fact]
        public void ShouldRecoverMapIndexFromLastCommitPoint()
        {
            var dataDir = NewDataPath("RecoverMapIndex");
            string indexFullPath;
            string commitPointsDirectory;
            var index = new MapRecoveryTestIndex();

            using (var server = GetNewServer(runInMemory: false, dataDirectory: dataDir))
            {
                CommitPointAfterFirstCommitOnly(server.SystemDatabase.Configuration);

                using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                {
                    index.Execute(store);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new Recovery // indexing this entity will create commit point
                        {
                            Name = "One",
                            Number = 1
                        });

                        session.SaveChanges();
                        WaitForIndexing(store);

                        session.Store(new Recovery // this will not be in commit point
                        {
                            Name = "Two",
                            Number = 2
                        });

                        session.SaveChanges();
                        WaitForIndexing(store);
                    }
                }

                Index indexInstance = server.SystemDatabase.IndexStorage.GetIndexInstance(index.IndexName);

                commitPointsDirectory = Path.Combine(server.SystemDatabase.Configuration.IndexStoragePath,
                                                     indexInstance.IndexId + "\\CommitPoints");

                indexFullPath = Path.Combine(server.SystemDatabase.Configuration.IndexStoragePath,
                                         indexInstance.IndexId.ToString(CultureInfo.InvariantCulture));
            }

            // make sure that there is only one commit point - which doesn't have the second entity indexed
            Assert.Equal(1, Directory.GetDirectories(commitPointsDirectory).Length);

            IndexMessing.MessSegmentsFile(indexFullPath);

            using (GetNewServer(runInMemory: false, dataDirectory: dataDir)) // do not delete previous directory
            {
                using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Query<Recovery, MapRecoveryTestIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
                        Assert.Equal(2, result.Count);
                    }
                }

                // here we should have another commit point because missing document after restore were indexed again
                Assert.Equal(2, Directory.GetDirectories(commitPointsDirectory).Length);
            }
        }

        [Fact]
        public void ShouldRecoverDeletes()
        {
            var dataDir = NewDataPath("ShouldRecoverDeletes");
            string indexFullPath;
            string commitPointsDirectory;
            var index = new MapRecoveryTestIndex();

            using (var server = GetNewServer(runInMemory: false, dataDirectory: dataDir))
            {
                CommitPointAfterFirstCommitOnly(server.SystemDatabase.Configuration);

                using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                {
                    index.Execute(store);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new Recovery
                        {
                            Name = "One",
                            Number = 1
                        });

                        session.Store(new Recovery
                        {
                            Name = "Two",
                            Number = 2
                        });

                        session.Store(new Recovery
                        {
                            Name = "Three",
                            Number = 3
                        });

                        session.SaveChanges(); // store all items in commit point
                        WaitForIndexing(store);

                        var itemToDelete = session.Query<Recovery, MapRecoveryTestIndex>().First();

                        session.Delete(itemToDelete);
                        session.SaveChanges();
                        WaitForIndexing(store);
                    }
                }

                Index indexInstance = server.SystemDatabase.IndexStorage.GetIndexInstance(index.IndexName);

                commitPointsDirectory = Path.Combine(server.SystemDatabase.Configuration.IndexStoragePath,
                                                     indexInstance.IndexId + "\\CommitPoints");

                indexFullPath = Path.Combine(server.SystemDatabase.Configuration.IndexStoragePath,
                                         indexInstance.IndexId.ToString(CultureInfo.InvariantCulture));
            }

            // make sure that there is only one commit point - which doesn't have the second entity indexed
            Assert.Equal(1, Directory.GetDirectories(commitPointsDirectory).Length);

            IndexMessing.MessSegmentsFile(indexFullPath);

            using (GetNewServer(runInMemory: false, dataDirectory: dataDir)) // do not delete previous directory
            {
                using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Query<Recovery, MapRecoveryTestIndex>().ToArray();
                        Assert.Equal(2, result.Length);
                    }
                }
            }
        }

        [Fact]
        public void ShouldDeleteCommitPointIfCouldNotRecoverFromIt()
        {
            var dataDir = NewDataPath("ShouldDeleteCommitPointIfCouldNotRecoverFromIt");
            string indexFullPath;
            string indexStoragePath;
            int indexId;
            string commitPointsDirectory;
            var index = new MapRecoveryTestIndex();

            using (var server = GetNewServer(runInMemory: false, dataDirectory: dataDir))
            {
                CommitPointAfterEachCommit(server.SystemDatabase.Configuration);

                using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                {
                    index.Execute(store);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new Recovery
                        {
                            Name = "One",
                            Number = 1
                        });

                        session.SaveChanges(); // first commit point
                        WaitForIndexing(store);

                        server.SystemDatabase.IndexStorage.FlushMapIndexes();
                        server.SystemDatabase.IndexStorage.FlushReduceIndexes();

                        session.Store(new Recovery
                        {
                            Name = "Two",
                            Number = 2
                        });

                        session.SaveChanges(); // second commit point
                        WaitForIndexing(store);

                        server.SystemDatabase.IndexStorage.FlushMapIndexes();
                        server.SystemDatabase.IndexStorage.FlushReduceIndexes();

                        session.Store(new Recovery
                        {
                            Name = "Three",
                            Number = 3
                        });

                        session.SaveChanges(); // second commit point
                        WaitForIndexing(store, timeout: TimeSpan.FromSeconds(60));

                        server.SystemDatabase.IndexStorage.FlushMapIndexes();
                        server.SystemDatabase.IndexStorage.FlushReduceIndexes();
                    }
                }
                Index indexInstance = server.SystemDatabase.IndexStorage.GetIndexInstance(index.IndexName);

                commitPointsDirectory = Path.Combine(server.SystemDatabase.Configuration.IndexStoragePath,
                                                     indexInstance.IndexId + "\\CommitPoints");

                indexFullPath = Path.Combine(server.SystemDatabase.Configuration.IndexStoragePath,
                                         indexInstance.IndexId.ToString(CultureInfo.InvariantCulture));

                indexStoragePath = server.SystemDatabase.Configuration.IndexStoragePath;

                indexId = indexInstance.IndexId;

            }

            // make sure that there are 3 commit points
            var directories = Directory.GetDirectories(commitPointsDirectory);
            Assert.Equal(3, directories.Length);

            // mess "index.CommitPoint" file in the SECOND and THIRD commit points by adding additional files required to recover from it

            for (int i = 1; i < 3; i++)
            {
                IndexCommitPoint commitPoint;
                Assert.True(IndexStorage.TryGetCommitPoint(new IndexCommitPointDirectory(indexStoragePath, indexId.ToString(CultureInfo.InvariantCulture), directories[i].Split(new[] { '\\' }).Last()), out commitPoint));

                commitPoint.SegmentsInfo.ReferencedFiles.Add("file-that-doesnt-exist");

                using (var commitPointFile = File.Open(Path.Combine(directories[i], "index.CommitPoint"), FileMode.Open))
                {
                    using (var sw = new StreamWriter(commitPointFile))
                    {
                        var textWriter = new JsonTextWriter(sw);

                        JsonExtensions.CreateDefaultJsonSerializer().Serialize(textWriter, commitPoint);
                        sw.Flush();
                    }
                }
            }

            IndexMessing.MessSegmentsFile(indexFullPath);

            using (GetNewServer(runInMemory: false, dataDirectory: dataDir)) // do not delete previous directory
            {
                using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                {
                    using (var session = store.OpenSession())
                    {
                        var result =
                            session.Query<Recovery, MapRecoveryTestIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();

                        Assert.Equal(3, result.Count);
                    }
                }
            }

            // there should be exactly 2 commit points:
            // the first one which we used to recover
            // and the second one created because of indexing after recovery
            Assert.Equal(2, Directory.GetDirectories(commitPointsDirectory).Length);
        }
    }
}
