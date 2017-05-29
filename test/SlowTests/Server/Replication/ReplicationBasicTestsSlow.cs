using System;
using System.Collections.Concurrent;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Http;
using Raven.Server;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ReplicationBasicTestsSlow : ReplicationTestsBase
    {
        public readonly string DbName = "TestDB" + Guid.NewGuid();

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task Master_master_replication_from_etag_zero_without_conflict_should_work(bool useSsl)
        {
            if (useSsl)
            {
                var tempPath = GenerateAndSaveSelfSignedCertificate();
                DoNotReuseServer(new ConcurrentDictionary<string, string>
                {
                    ["Raven/Certificate/Path"] = tempPath,
                    ["Raven/ServerUrl"] = "https://127.0.0.1:0"
                });
            }

            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
            {
                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);
                using (var session = store1.OpenSession())
                {
                    session.Store(new ReplicationBasicTests.User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");
                    session.SaveChanges();
                }
                using (var session = store2.OpenSession())
                {
                    session.Store(new ReplicationBasicTests.User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    }, "users/2");

                    session.SaveChanges();
                }

                var replicated1 = WaitForDocumentToReplicate<ReplicationBasicTests.User>(store1, "users/1", 10000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                var replicated2 = WaitForDocumentToReplicate<ReplicationBasicTests.User>(store1, "users/2", 10000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);

                replicated1 = WaitForDocumentToReplicate<ReplicationBasicTests.User>(store2, "users/1", 10000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                replicated2 = WaitForDocumentToReplicate<ReplicationBasicTests.User>(store2, "users/2", 10000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);
            }
        }

    }
}