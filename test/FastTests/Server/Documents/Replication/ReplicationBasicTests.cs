using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Xunit;

namespace FastTests.Server.Documents.Replication
{
    public class ReplicationBasicTests : RavenTestBase
    {
        public const string DbName = "TestDB";

        public class User
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Fact(Skip = "Not everything is done, WIP")]
        public async Task Single_way_replication_should_work()
        {
            var dbName1 = DbName + "1";
            var dbName2 = DbName + "2";
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

        private T WaitForDocumentToReplicate<T>(DocumentStore store, string id, int timeout)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
            {
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<T>(id);
                    if (doc != null)
                        return doc;
                }
                Thread.Sleep(25);
            }

            return default(T);
        }

        protected static void SetupReplication(string targetDbName, Raven.Client.Document.DocumentStore fromStore, Raven.Client.Document.DocumentStore toStore)
        {
            using (var session = fromStore.OpenSession())
            {
                session.Store(new ReplicationDocument
                {
                    Destinations = new List<ReplicationDestination>
                        {
                            new ReplicationDestination
                            {
                                Database = targetDbName,
                                Url = toStore.Url
                            }
                        }
                }, Constants.DocumentReplication.DocumentReplicationConfiguration);
                session.SaveChanges();
            }
        }
    }
}
