using System;
using System.Threading.Tasks;
using Xunit;

namespace FastTests.Server.Documents.Replication
{
    public class ReplicationBasicTests : ReplicationTestsBase
    {
        public readonly string DbName = "TestDB" + Guid.NewGuid();

        public class User
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Fact(Skip = "This test fails")]
        public async Task Master_master_replication_without_conflict_should_work()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName1))
            using (var store2 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName2))
            {
                store1.DefaultDatabase = dbName1;
                store2.DefaultDatabase = dbName2;

                SetupReplication(dbName2, store1, store2);
                SetupReplication(dbName1, store2, store1);
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
    


        [Fact(Skip = "This test fails")]
        public async Task Master_slave_replication_should_work()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName1))
            using (var store2 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName2))
            {
                store1.DefaultDatabase = dbName1;
                store2.DefaultDatabase = dbName2;

                SetupReplication(dbName2, store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    },"users/1");

                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    },"users/2");

                    session.SaveChanges();		
                }

                var replicated1 = WaitForDocumentToReplicate<User>(store2, "users/1", 10000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                var replicated2 = WaitForDocumentToReplicate<User>(store2, "users/2", 10000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);
            }

        }
    }
}
