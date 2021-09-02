using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions.Documents.Counters;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11571 : ClusterTestBase
    {
        public RavenDB_11571(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CounterStorage_Increment_ShouldCheckForOverflow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv"
                    }, "users/1");
                    session.CountersFor("users/1").Increment("Downloads", long.MaxValue);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var documentCounters = session.CountersFor("users/1");
                    var val = documentCounters.Get("Downloads");
                    Assert.Equal(long.MaxValue, val);

                    documentCounters.Increment("Downloads");
                    var e = Assert.Throws<CounterOverflowException>(() => session.SaveChanges());
                    Assert.Contains("CounterOverflowException: Could not increment counter 'Downloads' " +
                                    $"from document 'users/1' with value '{long.MaxValue}' by '1'.", e.Message);
                }
            }
        }

        [Fact]
        public async Task CountersHandler_GetCounterValue_ShouldCheckForOverflow()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var dbName = GetDatabaseName();

            var (_, servers) = await CreateDatabaseInCluster(new DatabaseRecord(dbName), 3, leader.WebUrl);

            using (var leaderStore = new DocumentStore
            {
                Database = dbName,
                Urls = new[] { leader.WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            using (var followerStore = new DocumentStore
            {
                Database = dbName,
                Urls = new[]
                {
                    servers.First(s => s.WebUrl != leader.WebUrl).WebUrl
                },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var stores = new[]
                {
                    (DocumentStore)leaderStore, (DocumentStore)followerStore
                };
                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv"
                    }, "users/1");

                    session.CountersFor("users/1").Increment("Downloads", long.MaxValue);
                    session.SaveChanges();

                    Assert.True(await WaitForCounterInClusterAsync<User>("users/1", stores));

                }

                foreach (var store in stores)
                {
                    using (var session = store.OpenSession())
                    {
                        var documentCounters = session.CountersFor("users/1");
                        var val = documentCounters.Get("Downloads");
                        Assert.Equal(long.MaxValue, val);
                    }
                }

                using (var session = followerStore.OpenSession())
                {
                    var documentCounters = session.CountersFor("users/1");
                    documentCounters.Increment("Downloads");
                    var e = Assert.Throws<CounterOverflowException>(() => session.SaveChanges());
                    Assert.Contains("CounterOverflowException: Overflow detected " +
                                    "in counter 'Downloads' from document 'users/1'"
                                    , e.Message);

                }
            }
        }

        private async Task<bool> WaitForCounterInClusterAsync<T>(string docId, IEnumerable<DocumentStore> stores)
        {
            var tasks = new List<Task<bool>>();

            foreach (var store in stores)
                tasks.Add(Task.Run(() => WaitForCounter<T>(store, docId)));

            await Task.WhenAll(tasks);

            return tasks.All(x => x.Result);
        }

        private bool WaitForCounter<T>(IDocumentStore store,
            string docId,
            int timeout = 10000)
        {
            if (DebuggerAttachedTimeout.DisableLongTimespan == false &&
                Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            Exception ex = null;
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession())
                {
                    try
                    {
                        var doc = session.Load<T>(docId);
                        if (doc != null &&
                            session.Advanced.GetCountersFor(doc) != null)
                            return true;

                    }
                    catch (Exception e)
                    {
                        ex = e;
                        // expected that we might get conflict, ignore and wait
                    }
                }

                Thread.Sleep(100);
            }

            using (var session = store.OpenSession())
            {
                //one last try, and throw if there is still a conflict
                var doc = session.Load<T>(docId);
                if (doc != null &&
                    session.Advanced.GetCountersFor(doc) != null)
                    return true;
            }
            if (ex != null)
            {
                throw ex;
            }
            return false;
        }
    }


}
