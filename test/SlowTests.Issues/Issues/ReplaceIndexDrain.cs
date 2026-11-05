using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class ReplaceIndexDrain : RavenTestBase
    {
        public ReplaceIndexDrain(ITestOutputHelper output) : base(output)
        {
        }

        private const string IndexName = "Orders/ByName";

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
        public async Task ReplacingAnIndexMustWaitForAReaderBeforeItRestartsTheEnvironment()
        {
            using (var store = GetDocumentStore(new Options { RunInMemory = false, Path = NewDataPath() }))
            {
                await using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 100; i++)
                        await bulk.StoreAsync(new Order { Name = "name" + i });
                }

                await PutIndex(store, "from o in docs.Orders select new { o.Name }");
                await Indexes.WaitForIndexingAsync(store);

                var database = await GetDatabase(store.Database);

                var probe = new Probe();
                var armed = 0;

                database.IndexStore.ForTestingPurposesOnly().DuringIndexReplacement_OnOldIndexDeletion += () =>
                {
                    if (Interlocked.Exchange(ref armed, 1) != 0)
                        return;

                    var replacement = database.IndexStore.GetIndex(IndexName);
                    probe.Thread = Thread.CurrentThread.Name;
                    probe.OnIndexingThread = replacement._indexingThread != null && Raven.Server.Utils.PoolOfThreads.LongRunningWork.Current == replacement._indexingThread;
                    probe.FlagOnThread = ReadFlag(replacement);
                    probe.Environment = replacement._environment;

                    var readerHolds = new ManualResetEventSlim(false);
                    var reader = new Thread(() =>
                    {
                        try
                        {
                            using (HoldReaderLock(replacement))
                            {
                                readerHolds.Set();

                                var deadline = DateTime.UtcNow.AddSeconds(5);
                                while (DateTime.UtcNow < deadline && probe.Environment.Disposed == false)
                                    Thread.Sleep(20);

                                probe.EnvironmentDisposedUnderReader = probe.Environment.Disposed;
                                try
                                {
                                    using (probe.Environment.ReadTransaction())
                                    {
                                    }
                                }
                                catch (Exception e)
                                {
                                    probe.ReaderError = e;
                                }
                            }
                        }
                        finally
                        {
                            readerHolds.Set();
                            probe.ReaderDone.Set();
                        }
                    }) { IsBackground = true };
                    reader.Start();
                    readerHolds.Wait(TimeSpan.FromSeconds(30));
                };

                await PutIndex(store, "from o in docs.Orders select new { o.Name, o.Name2 }");

                Assert.True(WaitForValue(() => Volatile.Read(ref armed) == 1, true, timeout: 120_000), "replacement never reached the hook");
                Assert.True(probe.ReaderDone.Wait(TimeSpan.FromSeconds(60)), "reader did not finish");

                Output.WriteLine($"thread={probe.Thread} onIndexingThread={probe.OnIndexingThread} flagOnThread={probe.FlagOnThread} " +
                                 $"envDisposedUnderReader={probe.EnvironmentDisposedUnderReader} readerError={probe.ReaderError?.GetType().Name}: {probe.ReaderError?.Message}");

                Assert.False(probe.EnvironmentDisposedUnderReader, "the environment was disposed while a reader held the index's reader lock");
                Assert.Null(probe.ReaderError);

                Assert.True(WaitForValue(() => database.IndexStore.GetIndex(IndexName)?.Definition.MapFields.ContainsKey("Name2") == true, true, timeout: 60_000));
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                    Assert.Equal(100, session.Query<Order>(IndexName).Customize(x => x.WaitForNonStaleResults()).ToList().Count);
            }
        }

        private sealed class Probe
        {
            public string Thread;
            public bool OnIndexingThread;
            public bool FlagOnThread;
            public Voron.StorageEnvironment Environment;
            public bool EnvironmentDisposedUnderReader;
            public Exception ReaderError;
            public readonly ManualResetEventSlim ReaderDone = new ManualResetEventSlim(false);
        }

        private static bool ReadFlag(Raven.Server.Documents.Indexes.Index index)
        {
            var field = typeof(Raven.Server.Documents.Indexes.Index).GetField("_isRunningQueriesWriteLockTaken", BindingFlags.NonPublic | BindingFlags.Instance);
            var asyncLocal = field.GetValue(index);
            return (bool)asyncLocal.GetType().GetProperty("Value").GetValue(asyncLocal);
        }

        private static IDisposable HoldReaderLock(Raven.Server.Documents.Indexes.Index index)
        {
            var field = typeof(Raven.Server.Documents.Indexes.Index).GetField("_currentlyRunningQueriesLock", BindingFlags.NonPublic | BindingFlags.Instance);
            var rwLock = field.GetValue(index);
            var method = rwLock.GetType().GetMethod("ReaderLock", new[] { typeof(CancellationToken) });
            return (IDisposable)method.Invoke(rwLock, new object[] { CancellationToken.None });
        }

        private static async Task PutIndex(IDocumentStore store, string map)
        {
            await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
            {
                Name = IndexName,
                Type = IndexType.Map,
                Maps = { map },
            }));
        }

        private sealed class Order
        {
            public string Name { get; set; }
            public string Name2 { get; set; }
        }
    }
}
