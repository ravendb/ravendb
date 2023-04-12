using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Cluster;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Cluster;

public class ClusterOperationTestsStress : ClusterTestBase
{
    public ClusterOperationTestsStress(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task NextIdentityForOperationShouldBroadcastAndFail()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var database = GetDatabaseName();
        var numberOfNodes = 3;
        var cluster = await CreateRaftCluster(numberOfNodes);
        var createResult = await CreateDatabaseInClusterInner(new DatabaseRecord(database), numberOfNodes, cluster.Leader.WebUrl, null);

        using (var store = new DocumentStore
        {
            Database = database,
            Urls = new[] { cluster.Leader.WebUrl }
        }.Initialize())
        {
            var result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|"));
            Assert.Equal(1, result);

            var node = createResult.Servers.First(n => n != cluster.Leader);
            node.ServerStore.InitializationCompleted.Reset(true);
            node.ServerStore.Initialized = false;

            await ActionWithLeader(DisposeServerAndWaitForFinishOfDisposalAsync);

            using (var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
            {
                await Task.WhenAll(createResult.Servers.Where(s => s.Disposed == false).Select(s => s.ServerStore.WaitForState(RachisState.Candidate, cancel.Token)));
            }

            var sp = Stopwatch.StartNew();
            var ex = Assert.Throws<AllTopologyNodesDownException>(() => result = store.Maintenance.ForDatabase(database).Send(new NextIdentityForOperation("person|")));
            Assert.True(sp.Elapsed < TimeSpan.FromSeconds(45));

            var ae = (AggregateException)ex.InnerException;
            Assert.NotNull(ae);

            var exceptionTypes = new List<Type>{
                    typeof(HttpRequestException),  // the disposed node
                    typeof(TimeoutException), // the hang node
                    typeof(RavenException) // the last active one (no leader)
                };

            Assert.Contains(ae.InnerExceptions[0].InnerException.GetType(), exceptionTypes);
            Assert.Contains(ae.InnerExceptions[1].InnerException.GetType(), exceptionTypes);
            Assert.Contains(ae.InnerExceptions[2].InnerException.GetType(), exceptionTypes);
        }
    }


    [Fact]
    public async Task ChangesApiReorderDatabaseNodes()
    {
        var db = "ReorderDatabaseNodes";
        var (_, leader) = await CreateRaftCluster(2);
        await CreateDatabaseInCluster(db, 2, leader.WebUrl);
        using (var store = new DocumentStore
        {
            Database = db,
            Urls = new[] { leader.WebUrl }
        }.Initialize())
        {
            var list = new BlockingCollection<DocumentChange>();
            var taskObservable = store.Changes();
            await taskObservable.EnsureConnectedNow();
            var observableWithTask = taskObservable.ForDocument("users/1");
            observableWithTask.Subscribe(list.Add);
            await observableWithTask.EnsureSubscribedNow();

            using (var session = store.OpenSession())
            {
                session.Store(new User(), "users/1");
                session.SaveChanges();
            }
            string url1 = store.GetRequestExecutor().Url;
            Assert.True(WaitForDocument(store, "users/1"));
            var value = WaitForValue(() => list.Count, 1);
            Assert.Equal(1, value);


            await ClusterOperationTests.ReverseOrderSuccessfully(store, db);

            var value2 = WaitForValue(() =>
            {
                string url2 = store.GetRequestExecutor().Url;
                return (url1 != url2);
            }, true);

            using (var session = store.OpenSession())
            {
                session.Store(new User(), "users/1");
                session.SaveChanges();
            }
            value = WaitForValue(() => list.Count, 2);
            Assert.Equal(2, value);
        }
    }
}
