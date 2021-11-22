using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17096 : ReplicationTestBase
    {
        public RavenDB_17096(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public async Task EtlFromReAddedNodeShouldWork()
        {
            const string mentorTag = "A";
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            using (var src = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            }))
            using (var dest = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            }))
            {
                var urls = nodes.Select(n => n.WebUrl).ToArray();
                var addEtl = await RavenDB_17311.AddEtl(src, dest.Database, urls, mentorTag);

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "Joe Doe", 30_000));

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);

                    session.Delete("users/1");
                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);

                    session.Store(new User()
                    {
                        Name = "John Dire"
                    }, "users/1");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "John Dire", 30_000));

                var deletion = await src.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(src.Database, hardDelete: true, fromNode: mentorTag,
                    timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));

                await WaitForRaftIndexToBeAppliedInCluster(deletion.RaftCommandIndex + 1, TimeSpan.FromSeconds(30));
                await RavenDB_7912.WaitForDatabaseToBeDeleted(leader, src.Database, TimeSpan.FromSeconds(15), CancellationToken.None);
                await WaitAndAssertForValueAsync(() => GetMembersCount(src), 2);

                var newResponsibleTag = WaitForNewResponsibleNode(src, addEtl.TaskId, mentorTag);
                Assert.NotNull(newResponsibleTag);

                var newResponsible = nodes.Single(s => s.ServerStore.NodeTag == newResponsibleTag);
                var db = await newResponsible.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(src.Database).ConfigureAwait(false);
                var etlDone = WaitForEtl(db, (s, statistics) => statistics.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);

                    session.Store(new User()
                    {
                        Name = "John Doe"
                    }, "users/2");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/2", u => u.Name == "John Doe", 30_000));
                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(10)));

                var addResult = await src.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(src.Database, node: mentorTag));
                await WaitForRaftIndexToBeAppliedInCluster(addResult.RaftCommandIndex, TimeSpan.FromSeconds(30));
                await WaitAndAssertForValueAsync(() => GetMembersCount(src), 3);

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);

                    session.Store(new User()
                    {
                        Name = "John Doe"
                    }, "marker");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "marker", u => u.Name == "John Doe", 30_000));
            }
        }

        private static string WaitForNewResponsibleNode(IDocumentStore store, long taskId, string oldTag, int timeout = 10_000)
        {
            var sw = Stopwatch.StartNew();
            while (true)
            {
                if (sw.ElapsedMilliseconds > timeout)
                    return null;

                var taskInfo = store.Maintenance.Send(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.RavenEtl));
                if (taskInfo.ResponsibleNode.NodeTag != oldTag)
                    return taskInfo.ResponsibleNode.NodeTag;

                Thread.Sleep(100);
            }
        }

        private static ManualResetEventSlim WaitForEtl(DocumentDatabase database, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var mre = new ManualResetEventSlim();

            database.EtlLoader.BatchCompleted += x =>
            {
                if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                    mre.Set();
            };

            return mre;
        }
    }
}
