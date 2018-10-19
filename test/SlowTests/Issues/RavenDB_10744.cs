using System;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10744 : RavenLowLevelTestBase
    {
        [Fact]
        public void Shold_stop_unloading_database_after_consecutive_corruptions_in_given_time()
        {
            UseNewLocalServer();

            using (var db = CreateDocumentDatabase())
            {
                var handler = db.ServerStore.DatabasesLandlord.CatastrophicFailureHandler;

                handler.TimeToWaitBeforeUnloadingDatabase = TimeSpan.Zero;
                var environmentId = Guid.NewGuid();

                CatastrophicFailureHandler.FailureStats failureStats;

                for (int i = 0; i < handler.MaxDatabaseUnloads; i++)
                {
                    handler.Execute(db.Name, new Exception("Catastrophic"), environmentId);

                    handler.TryGetStats(environmentId, out failureStats);

                    Assert.True(failureStats.WillUnloadDatabase);

                    var unloadTask = failureStats.DatabaseUnloadTask;

                    if (unloadTask != null) // could already unload it and null DatabaseUnloadTask 
                        Assert.True(unloadTask.Wait(TimeSpan.FromSeconds(30)));
                }

                handler.Execute(db.Name, new Exception("Catastrophic"), Guid.Empty);

                handler.TryGetStats(environmentId, out failureStats);

                Assert.False(failureStats.WillUnloadDatabase);

                // but it should let it unload after it exceeds the given time

                handler.NoFailurePeriod = TimeSpan.Zero;

                handler.Execute(db.Name, new Exception("Catastrophic"), environmentId);

                handler.TryGetStats(environmentId, out failureStats);

                Assert.True(failureStats.WillUnloadDatabase);
                Assert.True(failureStats.DatabaseUnloadTask.Wait(TimeSpan.FromSeconds(30)));
            }
        }

        [Fact]
        public void First_index_corruption_should_not_error_it_immediately()
        {
            UseNewLocalServer();

            using (var db = CreateDocumentDatabase())
            {
                using (var index = MapIndex.CreateNew(new IndexDefinition()
                {
                    Name = "Users_ByName",
                    Maps =
                    {
                        "from user in docs.Users select new { user.Name }"
                    },
                    Type = IndexType.Map
                }, db))
                {
                    PutUser(db);

                    index._indexStorage.SimulateCorruption = true;

                    index.Start();

                    // should unload db but not error the index
                    Assert.True(SpinWait.SpinUntil(() => db.ServerStore.DatabasesLandlord.DatabasesCache.Any() == false, TimeSpan.FromMinutes(1)));
                    Assert.Equal(IndexState.Normal, index.State);
                }
            }
        }

        [Fact]
        public void Should_be_able_to_read_index_stats_even_if_corruption_happened()
        {
            UseNewLocalServer();

            using (var db = CreateDocumentDatabase())
            {
                using (var index = MapIndex.CreateNew(new IndexDefinition()
                {
                    Name = "Users_ByName",
                    Maps =
                    {
                        "from user in docs.Users select new { user.Name }"
                    },
                    Type = IndexType.Map
                }, db))
                {
                    PutUser(db);

                    index._indexStorage.SimulateCorruption = true;

                    db.ServerStore.DatabasesLandlord.CatastrophicFailureHandler.MaxDatabaseUnloads = 0;

                    var mre = new ManualResetEventSlim();

                    db.Changes.OnIndexChange += change =>
                    {
                        if (change.Type == IndexChangeTypes.IndexMarkedAsErrored)
                            mre.Set();
                    };

                    index.Start();

                    Assert.True(mre.Wait(TimeSpan.FromMinutes(1)));
                    Assert.Equal(IndexState.Error, index.State);

                    long errorCount = 0;

                    for (int i = 0; i < 20; i++)
                    {
                        errorCount = index.GetErrorCount();

                        if (errorCount > 0)
                            break;

                        Thread.Sleep(500); // errors are updated in next tx when we update the stats
                    }
                    
                    Assert.Equal(1, errorCount);

                    using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                    {
                        using (context.OpenReadTransaction())
                        {
                            var indexStats = index.GetIndexStats(context);

                            Assert.True(indexStats.IsStale);
                        }
                    }

                    var indexingErrors = index.GetErrors();

                    Assert.Equal(1, indexingErrors.Count);
                }
            }
        }

        private static void PutUser(DocumentDatabase db)
        {
            using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                    {
                        ["Name"] = "John",
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            {[Constants.Documents.Metadata.Collection] = "Users"}
                    }))
                    {
                        db.DocumentsStorage.Put(context, "users/1", null, doc);
                    }

                    tx.Commit();
                }
            }
        }
    }
}
