using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Linq;
using Xunit;

namespace NewClientTests.NewClient.Server.Replication
{
    public class ReplicationBasicTests : ReplicationTestsBase
    {
        public readonly string DbName = "TestDB" + Guid.NewGuid();

        private class User
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Fact]
        public void Master_master_replication_from_etag_zero_without_conflict_should_work()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            {
                SetupReplication(store1, store2);
                SetupReplication(store2, store1);
                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");
                    session.SaveChanges();
                }
                using (var session = store2.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    }, "users/2");

                    session.SaveChanges();
                }

                var replicated1 = WaitForDocumentToReplicate<User>(store1, "users/1", 10000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                var replicated2 = WaitForDocumentToReplicate<User>(store1, "users/2", 10000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);

                replicated1 = WaitForDocumentToReplicate<User>(store2, "users/1", 10000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                replicated2 = WaitForDocumentToReplicate<User>(store2, "users/2", 10000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);
            }
        }

        [Fact]
        public void Master_slave_replication_from_etag_zero_should_work()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            {
                SetupReplication(store1, store2);

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

        [Fact]
        public void Master_slave_replication_with_multiple_PUTS_should_work()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            {
                SetupReplication(store1, store2);

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

                    Assert.True(docs.All(key => new[] { "John Dow", "Jane Dow", "Jack Dow", "Jessy Dow" }.Contains(key.Value.Name)));
                }

            }
        }

        [Fact]
        public void Master_master_replication_with_multiple_PUTS_should_work()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            {
                SetupReplication(store1, store2);
                SetupReplication(store2, store1);

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

                WaitForDocumentToReplicate<User>(store1, "users/1", 15000);
                WaitForDocumentToReplicate<User>(store1, "users/2", 15000);
                WaitForDocumentToReplicate<User>(store1, "users/3", 15000);
                WaitForDocumentToReplicate<User>(store1, "users/4", 15000);

                WaitForDocumentToReplicate<User>(store2, "users/1", 15000);
                WaitForDocumentToReplicate<User>(store2, "users/2", 15000);
                WaitForDocumentToReplicate<User>(store2, "users/3", 15000);
                WaitForDocumentToReplicate<User>(store2, "users/4", 15000);

                using (var session = store1.OpenSession())
                {
                    var docs = session.Load<User>(new[]
                    {
                        "users/1",
                        "users/2",
                        "users/3",
                        "users/4"
                    });

                    Assert.True(docs.All(key => new[] { "John Dow", "Jane Dow", "Jack Dow", "Jessy Dow" }.Contains(key.Value.Name)));
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

                    Assert.True(docs.All(key => new[] {"John Dow", "Jane Dow", "Jack Dow", "Jessy Dow"}.Contains(key.Value.Name)));
                }

            }
        }

        [Fact]
        public void Master_slave_replication_with_exceptions_should_work()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            {
                //TODO : configure test code to throw exceptions at server-side during replication
                //TODO : (find a way to do so)

                SetupReplication(store1, store2);

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

        [Fact]
        public void DeleteDestinationShouldWork()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            {
                SetupReplication(store1, store2);
                using (var session = store1.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout:TimeSpan.FromSeconds(30));
                    session.Store(new User
                    {
                        Name = "John Snow",
                        Age = 30
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                }

                DeleteReplication(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges();
                    session.Store(new User
                    {
                        Name = "Ghost",
                        Age = 30
                    }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    Assert.Null(session.Load<User>("users/2"));
                }
            }
        }

        [Fact]
        public void DisableDestinationShouldWork()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            var dbName3 = DbName + "-3";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            using (var store3 = GetDocumentStore(dbSuffixIdentifier: dbName3))
            {
                SetupReplication(store1, store2, store3);
                using (var session = store1.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User
                    {
                        Name = "John Snow",
                        Age = 30
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }

                using (var session = store2.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }

                EnableOrDisableReplication(store1, store2, true);

                using (var session = store1.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges();
                    session.Store(new User
                    {
                        Name = "Ghost",
                        Age = 30
                    }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    Assert.Null(session.Load<User>("users/2"));
                }

                using (var session = store3.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/2"));
                }

                EnableOrDisableReplication(store1, store2);
                using (var session = store1.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges();
                    session.Store(new User
                    {
                        Name = "John Snow",
                        Age = 30
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }
            }
        }
    }
}
