using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    public class ShardedEtlTests : ShardedTestBase
    {
        public ShardedEtlTests(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public void RavenEtl_Unsharded_Destination()
        {
            using (var src = GetShardedDocumentStore())
            using (var dest = GetDocumentStore())
            {
                Server.ServerStore.Engine.Timeout.Disable = true;

                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                const string id = "users/1";

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Joe Doe"
                    }, id);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(id);

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete(id);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(id);

                    Assert.Null(user);
                }
            }
        }

        [Fact]
        public void RavenEtl_Unsharded_Destination2()
        {
            using (var src = GetShardedDocumentStore())
            using (var dest = GetDocumentStore())
            {
                Server.ServerStore.Engine.Timeout.Disable = true;

                //AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                //const string id = "users/1";

                var dbRecord = src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(src.Database)).Result;
                var shardedCtx = new ShardedContext(Server.ServerStore, dbRecord);
                var ids = new[] {"users/0", "users/4", "users/1"};

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    for (int i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var shardIndex = shardedCtx.GetShardIndex(context, id);
                        Assert.Equal(i, shardIndex);
                    }
                }

                using (var session = src.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        session.Store(new User
                        {
                            Name = "User" + i
                        }, id);
                    }

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var user = session.Load<User>(id);

                        Assert.NotNull(user);
                        Assert.Equal("User" + i, user.Name);
                    }
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete(ids[1]);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(ids[1]);

                    Assert.Null(user);
                }
            }
        }

        private static AddEtlOperationResult AddEtl(IDocumentStore src, IDocumentStore dst, string collection, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null)
        {
            var connectionStringName = $"{src.Database}@{src.Urls.First()} to {dst.Database}@{dst.Urls.First()}";

            return AddEtl(src, new RavenEtlConfiguration()
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>{ collection },
                            Script = script,
                            ApplyToAllDocuments = applyToAllDocuments,
                            Disabled = disabled
                        }
                    },
                    MentorNode = mentor,
                },
                new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dst.Database,
                    TopologyDiscoveryUrls = dst.Urls,
                }
            );
        }

        private static AddEtlOperationResult AddEtl<T>(IDocumentStore src, EtlConfiguration<T> configuration, T connectionString) where T : ConnectionString
        {
            var putResult = src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var dbRecord = src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(src.Database)).Result;
            Assert.Equal(1, dbRecord.RavenConnectionStrings.Count);


            var addResult = src.Maintenance.Send(new AddEtlOperation<T>(configuration));
            return addResult;
        }

        private ManualResetEventSlim WaitForEtl(IDocumentStore store, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var dbs = Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database).ToList();

            var mre = new ManualResetEventSlim();
            foreach (var task in dbs)
            {
                var db = task.Result;
                db.EtlLoader.BatchCompleted += x =>
                {
                    if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                        mre.Set();
                };

            }

            return mre;
        }
    }
}
