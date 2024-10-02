using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL;

public class RavenDB_20757 : ReplicationTestBase
{
    public RavenDB_20757(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task OnFailureToUpdateProcessStateEtlShouldEnterFallbackMode()
    {
        var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0,
            customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "5"
            }, 
            shouldRunInMemory: false);

        var mentor = nodes[1];
        var destNode = nodes[2];

        using (var src = GetDocumentStore(new Options
        {
            Server = leader,
            ReplicationFactor = 3,
            RunInMemory = false
        }))
        using (var dest = GetDocumentStore(new Options
        {
            Server = destNode,
            ReplicationFactor = 1,
            ModifyDatabaseRecord = r => r.Topology = new DatabaseTopology
            {
               Members = new List<string>{ destNode.ServerStore.NodeTag }
            },
            RunInMemory = false
        }))
        {
            AddEtl(src, dest, mentor.ServerStore.NodeTag);

            var etlDone = await WaitForEtl(mentor, src.Database);

            using (var session = src.OpenSession())
            {
                session.Store(new User(), "users/1");
                session.SaveChanges();
            }

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromSeconds(10)));

            var destDb = await GetDatabase(destNode, dest.Database);

            long etag;
            using (var session = dest.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                var cvString = session.Advanced.GetChangeVectorFor(user);
                etag = ChangeVectorUtils.GetEtagById(cvString, destDb.DbBase64Id);
            }

            // dispose leader
            var disposeResult = await DisposeServerAndWaitForFinishOfDisposalAsync(leader);

            etlDone.Reset();
            using (var session = src.OpenSession())
            {
                var user = session.Load<User>("users/1");
                user.Name = "Jerry";
                session.SaveChanges();
            }

            // etl should reach destination but fail to complete batch (can't update process state)
            Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "Jerry"));
            var timeout = mentor.ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan * 3;
            Assert.False(await etlDone.WaitAsync(timeout));

            // assert that the same modification wasn't sent more than once
            using (var session = dest.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                var cvString = session.Advanced.GetChangeVectorFor(user);
                var newEtag = ChangeVectorUtils.GetEtagById(cvString, destDb.DbBase64Id);

                Assert.Equal(etag + 1, newEtag);

                etag = newEtag;
            }

            var srcDb = await GetDatabase(mentor, src.Database);
            var procState = srcDb.EtlLoader.Processes.FirstOrDefault();
            Assert.NotNull(procState);

            var stats = procState.GetPerformanceStats()
                .Where(s => s.LastLoadedEtag > 0)
                .OrderBy(s => s.Id)
                .ToList();

            Assert.NotEmpty(stats);

            var current = stats[0];
            for (int i = 1; i < stats.Count; i++)
            {
                var next = stats[i];
                Assert.True(current.LastLoadedEtag < next.LastLoadedEtag);
                current = next;
            }

            // revive node
            GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    { RavenConfiguration.GetKey(x => x.Core.ServerUrls), leader.WebUrl }
                },
                RunInMemory = false,
                DeletePrevious = false,
                DataDirectory = disposeResult.DataDirectory
            });

            // now etl should complete batch successfully  
            Assert.True(await etlDone.WaitAsync(TimeSpan.FromSeconds(60)));
            etlDone.Reset();

            using (var session = src.OpenSession())
            {
                var user = session.Load<User>("users/1");
                user.LastName = "Garcia";
                session.SaveChanges();
            }

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromSeconds(10)));

            using (var session = dest.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                Assert.Equal("Garcia", user.LastName);

                var cvString = session.Advanced.GetChangeVectorFor(user);
                var newEtag = ChangeVectorUtils.GetEtagById(cvString, destDb.DbBase64Id);

                Assert.Equal(etag + 1, newEtag);
            }
        }
    }

    private static void AddEtl(IDocumentStore src, IDocumentStore destination, string mentor)
    {
        const string connectionStringName = "cs";
        var connectionString = new RavenConnectionString
        {
            Name = connectionStringName, 
            Database = destination.Database, 
            TopologyDiscoveryUrls = destination.Urls
        };

        var result = src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
        Assert.NotNull(result.RaftCommandIndex);

        src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(new RavenEtlConfiguration
        {
            Name = connectionStringName,
            ConnectionStringName = connectionStringName,
            Transforms =
            {
                new Transformation
                {
                    Name = $"ETL : {connectionStringName}",
                    Collections = new List<string>(new[] { "Users" }),
                    Script = null,
                    ApplyToAllDocuments = false,
                    Disabled = false
                }
            },
            MentorNode = mentor
        }));
    }

    private static async Task<AsyncManualResetEvent> WaitForEtl(RavenServer server, string database)
    {
        var documentDatabase = await GetDatabase(server, database);
        var mre = new AsyncManualResetEvent();
        documentDatabase.EtlLoader.BatchCompleted += x =>
        {
            if (x.Statistics.LoadSuccesses > 0)
                mre.Set();
        };
        return mre;
    }
}
