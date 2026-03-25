using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_8866 : RavenTestBase
    {
        public RavenDB_8866(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task CanResetEtl()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var runs = 0;

                var etlDone = Etl.WaitForEtlToComplete(src);

                var resetDone = Etl.WaitForEtlToComplete(src, (n, statistics) => ++runs >= 2);

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "allUsers",
                            Collections = { "Users" }
                        }
                    }
                };

                Etl.AddEtl(src, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

                await src.Maintenance.SendAsync(new ResetEtlOperation("myConfiguration", "allUsers"));

                Assert.True(await resetDone.WaitAsync(TimeSpan.FromMinutes(1)));
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task CanResetEtl2()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "allUsers",
                            Collections = { "Users" }
                        }
                    }
                };

                var mre = new ManualResetEvent(true);
                var mre2 = new ManualResetEvent(false);
                var etlDone = Etl.WaitForEtlToComplete(src, (n, s) =>
                {
                    Assert.True(mre.WaitOne(TimeSpan.FromMinutes(1)));
                    mre.Reset();

                    mre2.Set();

                    return true;
                });

                Etl.AddEtl(src, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                var set = new HashSet<string>
                {
                    "asd"
                };

                for (int i = 0; i < 10; i++)
                {
                    Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)), $"blah at {i}");

                    mre.Set();

                    Assert.True(mre2.WaitOne(TimeSpan.FromMinutes(1)), $"oops at {i}");
                    mre2.Reset();

                    var t1 = src.Maintenance.SendAsync(new ResetEtlOperation("myConfiguration", "allUsers"));

                    for (int j = 0; j < 100; j++)
                    {
                        var t2 = src.Maintenance.Server.SendAsync(new UpdateUnusedDatabasesOperation(src.Database, set));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl, Skip = "RavenDB-14127")]
        public async Task CanResetEtl3()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms = { new Transformation() { Name = "allUsers", Collections = { "Users" } } }
                };

                Etl.AddEtl(src, configuration, new RavenConnectionString { Name = "test", TopologyDiscoveryUrls = dest.Urls, Database = dest.Database, });

                var t = Task.Run(async () =>
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await src.Maintenance.SendAsync(new ResetEtlOperation("myConfiguration", "allUsers"));
                        await Task.Delay(100);
                    }
                });

                var indexes = new List<Task>();
                for (int i = 0; i < 100; i++)
                {
                    var index = new Index($"test{i}");
                    indexes.Add(index.ExecuteAsync(src));
                }

                await Task.WhenAll(indexes);
                await t;

                var record = await src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(src.Database));
                Assert.Equal(100, record.Indexes.Count);
            }
        }

        public class MyEntity
        {
            public string Id { get; set; }
            public string AuthorId { get; set; }
            public string Title { get; set; }
            public string Language { get; set; }
        }

        public class Index : AbstractIndexCreationTask<MyEntity>
        {
            private readonly string _indexName;

            public override string IndexName => _indexName ?? base.IndexName;

            public Index(string name) : this()
            {
                _indexName = name;
            }

            public Index()
            {
                Map = entities => from e in entities
                    select new
                    {
                        e.AuthorId,
                        e.Title,
                        e.Language,
                    };
            }
        }
    }
}
