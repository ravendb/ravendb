using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Replication;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Replication
{
    public class ReplicationBasicTests : ReplicationTestBase
    {
        public ReplicationBasicTests(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }
     
        [RavenTheory(RavenTestCategory.Replication, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Master_slave_replication_from_etag_zero_should_work(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    }, "users/2");

                    session.SaveChanges();
                }

                var replicated1 = WaitForDocumentToReplicate<User>(store2, "users/1", 15000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                var replicated2 = WaitForDocumentToReplicate<User>(store2, "users/2", 5000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);
            }
        }

        [RavenTheory(RavenTestCategory.Replication, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Master_slave_replication_with_multiple_PUTS_should_work(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    }, "users/2");

                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jack Dow",
                        Age = 33
                    }, "users/3");

                    session.Store(new User
                    {
                        Name = "Jessy Dow",
                        Age = 34
                    }, "users/4");

                    session.SaveChanges();
                }

                WaitForDocumentToReplicate<User>(store2, "users/1", 15000);
                WaitForDocumentToReplicate<User>(store2, "users/2", 15000);
                WaitForDocumentToReplicate<User>(store2, "users/3", 15000);
                WaitForDocumentToReplicate<User>(store2, "users/4", 15000);

                using (var session = store2.OpenSession())
                {
                    var docs = session.Load<User>(new[]
                    {
                        "users/1",
                        "users/2",
                        "users/3",
                        "users/4"
                    });

                    Assert.Contains(docs, d => d.Value.Name.Equals("John Dow"));
                    Assert.Contains(docs, d => d.Value.Name.Equals("Jane Dow"));
                    Assert.Contains(docs, d => d.Value.Name.Equals("Jack Dow"));
                    Assert.Contains(docs, d => d.Value.Name.Equals("Jessy Dow"));
                }

            }
        }

        [RavenTheory(RavenTestCategory.Replication, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Master_master_replication_with_multiple_PUTS_should_work(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    }, "users/2");

                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jack Dow",
                        Age = 33
                    }, "users/3");

                    session.Store(new User
                    {
                        Name = "Jessy Dow",
                        Age = 34
                    }, "users/4");

                    session.SaveChanges();
                }
                
                Assert.NotNull(WaitForDocumentToReplicate<User>(store1, "users/1", 15000));
                Assert.NotNull(WaitForDocumentToReplicate<User>(store1, "users/2", 15000));
                Assert.NotNull(WaitForDocumentToReplicate<User>(store1, "users/3", 15000));
                Assert.NotNull(WaitForDocumentToReplicate<User>(store1, "users/4", 15000));

                Assert.NotNull(WaitForDocumentToReplicate<User>(store2, "users/1", 15000));
                Assert.NotNull(WaitForDocumentToReplicate<User>(store2, "users/2", 15000));
                Assert.NotNull(WaitForDocumentToReplicate<User>(store2, "users/3", 15000));
                Assert.NotNull(WaitForDocumentToReplicate<User>(store2, "users/4", 15000));

                using (var session = store1.OpenSession())
                {
                    var docs = session.Load<User>(new[]
                    {
                        "users/1",
                        "users/2",
                        "users/3",
                        "users/4"
                    });

                    Assert.Contains(docs, d => d.Value.Name.Equals("John Dow"));
                    Assert.Contains(docs, d => d.Value.Name.Equals("Jane Dow"));
                    Assert.Contains(docs, d => d.Value.Name.Equals("Jack Dow"));
                    Assert.Contains(docs, d => d.Value.Name.Equals("Jessy Dow"));
                }

                using (var session = store2.OpenSession())
                {
                    var docs = session.Load<User>(new[]
                    {
                        "users/1",
                        "users/2",
                        "users/3",
                        "users/4"
                    });

                    Assert.Contains(docs, d => d.Value.Name.Equals("John Dow"));
                    Assert.Contains(docs, d => d.Value.Name.Equals("Jane Dow"));
                    Assert.Contains(docs, d => d.Value.Name.Equals("Jack Dow"));
                    Assert.Contains(docs, d => d.Value.Name.Equals("Jessy Dow"));
                }

            }
        }

        [RavenTheory(RavenTestCategory.Replication, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Master_slave_replication_with_exceptions_should_work(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                //TODO : configure test code to throw exceptions at server-side during replication
                //TODO : (find a way to do so)

                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    }, "users/2");

                    session.SaveChanges();
                }

                var replicated1 = WaitForDocumentToReplicate<User>(store2, "users/1", 15000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                var replicated2 = WaitForDocumentToReplicate<User>(store2, "users/2", 5000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);
            }
        }

        [RavenTheory(RavenTestCategory.Replication, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Can_get_performance_stats(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                await SetupReplicationAsync(store1, store2); // master-slave

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    }, "users/2");

                    session.SaveChanges();
                }

                Assert.NotNull(WaitForDocumentToReplicate<User>(store2, "users/1", 15000));
                Assert.NotNull(WaitForDocumentToReplicate<User>(store2, "users/2", 5000));

                var op1 = store1.Maintenance;
                var op2 = store2.Maintenance;

                if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                {
                    op1 = op1.ForNode("A").ForShard(0);
                    op2 = op2.ForNode("A").ForShard(0);
                }

                var stats1 = op1.Send(new GetReplicationPerformanceStatisticsOperation());
                var stats2 = op2.Send(new GetReplicationPerformanceStatisticsOperation());

                Assert.NotEmpty(stats1.Outgoing);
                Assert.Empty(stats1.Incoming);

                Assert.Empty(stats2.Outgoing);
                Assert.NotEmpty(stats2.Incoming);
            }
        }
    }
}
