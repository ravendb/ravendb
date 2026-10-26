using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.QueueSink.Commands;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_26232 : ReplicationTestBase
    {
        public RavenDB_26232(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Counters)]
        public async Task SmugglerCounterBatchCommand_CanBeReExecutedByTheTransactionMerger()
        {
            var file = GetTempFileName();

            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");
                        session.CountersFor("users/1").Increment("Likes", 10);
                        session.CountersFor("users/1").Increment("Dislikes", 20);
                        session.CountersFor("users/2").Increment("Downloads", 30);
                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    // 'users/1' already has counters in the destination (with the destination's db id),
                    // so the imported counter group is merged with the existing one
                    using (var session = store2.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        session.CountersFor("users/1").Increment("Likes", 1);
                        session.CountersFor("users/1").Increment("Downloads", 5);
                        await session.SaveChangesAsync();
                    }

                    var database = await GetDatabase(store2.Database);

                    SmugglerResult result = null;
                    var failedCommands = await ExecuteWithForcedRetryAsync(database, command => command is CountersHandler.SmugglerCounterBatchCommand, async () =>
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        result = (SmugglerResult)await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                    });

                    var command = (CountersHandler.SmugglerCounterBatchCommand)Assert.Single(failedCommands);
                    Assert.True(command.IsDisposed);
                    Assert.Equal(0, result.Counters.ErroredCount);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        Assert.Equal(new[] { "Dislikes", "Downloads", "Likes" }, session.Advanced.GetCountersFor(user1).OrderBy(x => x));

                        var counters = await session.CountersFor(user1).GetAllAsync();
                        Assert.Equal(3, counters.Count);
                        Assert.Equal(11, counters["Likes"]);
                        Assert.Equal(20, counters["Dislikes"]);
                        Assert.Equal(5, counters["Downloads"]);

                        var user2 = await session.LoadAsync<User>("users/2");
                        Assert.Equal(new[] { "Downloads" }, session.Advanced.GetCountersFor(user2));
                        Assert.Equal(30, await session.CountersFor(user2).GetAsync("Downloads"));
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Counters)]
        public async Task SmugglerCounterBatchCommand_FixingTheCounterNames_ShouldSurviveReExecutionByTheTransactionMerger()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                    session.CountersFor("users/1").Increment("Likes", 10);
                    session.CountersFor("users/1").Increment("Dislikes", 20);
                    await session.SaveChangesAsync();
                }

                var database = await GetDatabase(store.Database);
                var result = new SmugglerResult();

                var command = new CountersHandler.SmugglerCounterBatchCommand(database, result);
                using (command)
                {
                    using (command.Context.OpenReadTransaction())
                    {
                        var counterGroup = database.DocumentsStorage.CountersStorage.GetCountersFrom(command.Context, 0, 0, int.MaxValue).Single();

                        // drop the original counter names, the command has to restore them from the document
                        counterGroup.Values.Modifications = new DynamicJsonValue(counterGroup.Values)
                        {
                            [CountersStorage.CounterNames] = new DynamicJsonValue()
                        };

                        command.Add(new CounterGroupDetail
                        {
                            DocumentId = command.Context.GetLazyString("users/1"),
                            Values = command.Context.ReadObject(counterGroup.Values, "users/1")
                        });
                    }

                    var failedCommands = await ExecuteWithForcedRetryAsync(database, c => ReferenceEquals(c, command), () => database.TxMerger.Enqueue(command));
                    Assert.Same(command, Assert.Single(failedCommands));
                }

                Assert.True(command.IsDisposed);
                Assert.Equal(0, result.Counters.ErroredCount);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(new[] { "Dislikes", "Likes" }, session.Advanced.GetCountersFor(user).OrderBy(x => x));

                    var counters = await session.CountersFor(user).GetAllAsync();
                    Assert.Equal(2, counters.Count);
                    Assert.Equal(10, counters["Likes"]);
                    Assert.Equal(20, counters["Dislikes"]);
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.TimeSeries)]
        public async Task SmugglerTimeSeriesBatchCommand_CanBeReExecutedByTheTransactionMerger()
        {
            // enough documents (each one with a time series) to require more than one import batch (the batch size is 16 MB, each new document accounts for 2 KB)
            const int numberOfItems = 9_000;

            var file = GetTempFileName();
            var baseline = RavenTestHelper.UtcToday;

            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        var heartrate = session.TimeSeriesFor("users/1", "Heartrate");
                        for (int i = 0; i < 360; i++)
                        {
                            heartrate.Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                        }

                        var bloodPressure = session.TimeSeriesFor("users/1", "BloodPressure");
                        for (int i = 0; i < 10; i++)
                        {
                            bloodPressure.Append(baseline.AddMinutes(i), new[] { 120d + i, 80d + i }, "watches/2");
                        }

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        // creates a deleted range that is exported as well
                        session.TimeSeriesFor("users/1", "Heartrate").Delete(baseline.AddSeconds(1000), baseline.AddSeconds(2000));
                        await session.SaveChangesAsync();
                    }

                    using (var bulkInsert = store1.BulkInsert())
                    {
                        for (var i = 0; i < numberOfItems; i++)
                        {
                            var id = $"items/{i}";
                            await bulkInsert.StoreAsync(new User { Name = $"Item{i}" }, id);

                            using (var timeSeries = bulkInsert.TimeSeriesFor(id, "Count"))
                            {
                                await timeSeries.AppendAsync(baseline, i);
                            }
                        }
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    TimeSeriesEntry[] expectedHeartrate;
                    TimeSeriesEntry[] expectedBloodPressure;
                    using (var session = store1.OpenAsyncSession())
                    {
                        expectedHeartrate = await session.TimeSeriesFor("users/1", "Heartrate").GetAsync();
                        expectedBloodPressure = await session.TimeSeriesFor("users/1", "BloodPressure").GetAsync();
                    }

                    var expectedStats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(numberOfItems + 1, expectedStats.CountOfDocuments);

                    var database = await GetDatabase(store2.Database);

                    // the first import appends entire segments (the time series don't exist yet),
                    // the second one appends the values of each segment to the existing time series
                    for (var attempt = 0; attempt < 2; attempt++)
                    {
                        SmugglerResult result = null;
                        var failedCommands = await ExecuteWithForcedRetryAsync(database, command => command is TimeSeriesHandler.SmugglerTimeSeriesBatchCommand, async () =>
                        {
                            operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                            result = (SmugglerResult)await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
                        });

                        Assert.True(failedCommands.Count >= 2, $"attempt: {attempt}, failed commands: {failedCommands.Count}");
                        Assert.All(failedCommands, command => Assert.True(((TimeSeriesHandler.SmugglerTimeSeriesBatchCommand)command).IsDisposed));
                        Assert.Equal(0, result.TimeSeries.ErroredCount);

                        var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                        Assert.Equal(expectedStats.CountOfDocuments, stats.CountOfDocuments);
                        Assert.Equal(expectedStats.CountOfTimeSeriesSegments, stats.CountOfTimeSeriesSegments);

                        using (var session = store2.OpenAsyncSession())
                        {
                            var user = await session.LoadAsync<User>("users/1");
                            Assert.Equal(new[] { "BloodPressure", "Heartrate" }, session.Advanced.GetTimeSeriesFor(user).OrderBy(x => x));

                            AssertTimeSeriesEntries(expectedHeartrate, await session.TimeSeriesFor("users/1", "Heartrate").GetAsync());
                            AssertTimeSeriesEntries(expectedBloodPressure, await session.TimeSeriesFor("users/1", "BloodPressure").GetAsync());

                            foreach (var i in new[] { 0, numberOfItems / 2, numberOfItems - 1 })
                            {
                                var entry = Assert.Single(await session.TimeSeriesFor($"items/{i}", "Count").GetAsync());
                                Assert.Equal(baseline, entry.Timestamp);
                                Assert.Equal(i, entry.Value);
                            }
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Counters)]
        public async Task MergedDocumentReplicationCommand_MergingCounters_CanBeReExecutedByTheTransactionMerger()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                Assert.NotNull(await Replication.WaitForDocumentToReplicateAsync<User>(store2, "users/1", timeout: 15_000));

                // the destination has its own counter group for 'users/1' (with its own db id),
                // the replicated counter group is merged into it
                using (var session = store2.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("Likes", 1);
                    session.CountersFor("users/1").Increment("Downloads", 5);
                    await session.SaveChangesAsync();
                }

                var database2 = await GetDatabase(store2.Database);
                var failedCommands = await ExecuteWithForcedRetryAsync(database2, command => command is IncomingReplicationHandler.MergedDocumentReplicationCommand, async () =>
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("Likes", 10);
                        session.CountersFor("users/1").Increment("Dislikes", 20);
                        await session.SaveChangesAsync();
                    }

                    Assert.True(Replication.WaitForCounterReplication(new[] { store2 }, "users/1", "Likes", expected: 11, TimeSpan.FromSeconds(30)));
                    Assert.True(Replication.WaitForCounterReplication(new[] { store2 }, "users/1", "Dislikes", expected: 20, TimeSpan.FromSeconds(30)));
                });

                Assert.NotEmpty(failedCommands);

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(new[] { "Dislikes", "Downloads", "Likes" }, session.Advanced.GetCountersFor(user).OrderBy(x => x));

                    var counters = await session.CountersFor(user).GetAllAsync();
                    Assert.Equal(3, counters.Count);
                    Assert.Equal(11, counters["Likes"]);
                    Assert.Equal(20, counters["Dislikes"]);
                    Assert.Equal(5, counters["Downloads"]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public async Task BatchQueueSinkScriptCommand_CanBeReExecutedByTheTransactionMerger()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var messages = new List<BlittableJsonReaderObject>();
                    for (var i = 0; i < 3; i++)
                    {
                        messages.Add(context.ReadObject(new DynamicJsonValue
                        {
                            ["Id"] = $"users/{i}",
                            ["Name"] = $"Name{i}"
                        }, "message"));
                    }

                    messages.Add(context.ReadObject(new DynamicJsonValue
                    {
                        ["Id"] = "users/invalid",
                        ["Name"] = "invalid"
                    }, "message"));

                    var command = new BatchQueueSinkScriptCommand("if (this.Name === 'invalid') throw 'invalid message'; put(this.Id, this);", messages, logger: null);

                    var failedCommands = await ExecuteWithForcedRetryAsync(database, c => ReferenceEquals(c, command), () => database.TxMerger.Enqueue(command));
                    Assert.Same(command, Assert.Single(failedCommands));

                    // the results of the failed execution must not be counted
                    Assert.Equal(3, command.ProcessedSuccessfully);
                    Assert.Equal(1, command.ScriptErrors.Count);

                    // the command must not dispose the messages, they are owned by the caller
                    foreach (var message in messages)
                        Assert.True(message.TryGet("Id", out string _));
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 3; i++)
                    {
                        var user = await session.LoadAsync<User>($"users/{i}");
                        Assert.Equal($"Name{i}", user.Name);
                    }

                    Assert.Null(await session.LoadAsync<User>("users/invalid"));
                }
            }
        }

#if DEBUG
        [RavenFact(RavenTestCategory.Core)]
        public async Task TransactionMerger_ShouldDetectDisposalOfObjectsThatAreNotOwnedByTheTransactionContext()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var ownedByTheCommand = context.ReadObject(new DynamicJsonValue { ["Name"] = "test" }, "doc");

                    var e = await Assert.ThrowsAsync<InvalidOperationException>(() => database.TxMerger.Enqueue(new DisposingCommand(ownedByTheCommand)));
                    Assert.Contains("restricted", e.Message);

                    // objects that are owned by the transaction context can be disposed
                    await database.TxMerger.Enqueue(new DisposingCommand(toDispose: null));
                }
            }
        }
#endif

        private static void AssertTimeSeriesEntries(TimeSeriesEntry[] expected, TimeSeriesEntry[] actual)
        {
            Assert.Equal(expected.Length, actual.Length);

            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i].Timestamp, actual[i].Timestamp);
                Assert.Equal(expected[i].Values, actual[i].Values);
                Assert.Equal(expected[i].Tag, actual[i].Tag);
            }
        }

        /// <summary>
        /// Executes the given action while every command that matches the predicate fails (once) as part of a merged transaction,
        /// which makes the transaction merger execute the same command instance again, independently.
        /// </summary>
        /// <returns>The commands that were failed</returns>
        private static async Task<List<MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>> ExecuteWithForcedRetryAsync(
            DocumentDatabase database,
            Func<MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, bool> shouldFail,
            Func<Task> action)
        {
            var failedCommands = new List<MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>();

            var testingStuff = database.TxMerger.ForTestingPurposesOnly();
            testingStuff.RerunSingleCommandIndependentlyOnFailure = true;
            testingStuff.AfterExecutingCommandInMergedTransaction = command =>
            {
                if (shouldFail(command) == false)
                    return;

                lock (failedCommands)
                {
                    if (failedCommands.Any(x => ReferenceEquals(x, command)))
                        return;

                    failedCommands.Add(command);
                }

                throw new SimulatedMergedTransactionFailureException();
            };

            try
            {
                await action();
            }
            finally
            {
                testingStuff.AfterExecutingCommandInMergedTransaction = null;
                testingStuff.RerunSingleCommandIndependentlyOnFailure = false;
            }

            lock (failedCommands)
            {
                return failedCommands.ToList();
            }
        }

        private class SimulatedMergedTransactionFailureException : Exception
        {
        }

        private class DisposingCommand : DocumentMergedTransactionCommand
        {
            private readonly BlittableJsonReaderObject _toDispose;

            public DisposingCommand(BlittableJsonReaderObject toDispose)
            {
                _toDispose = toDispose;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var toDispose = _toDispose ?? context.ReadObject(new DynamicJsonValue { ["Name"] = "test" }, "doc");
                toDispose.Dispose();
                return 1;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
            {
                throw new NotImplementedException();
            }
        }
    }
}
