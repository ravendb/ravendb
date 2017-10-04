using System;
using System.Collections.Generic;
using System.Net;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bundles.Replication
{
    //tests for http://issues.hibernatingrhinos.com/issue/RavenDB-3229
    public class CollectionSpecific : ReplicationBase
    {
        private DocumentStore store3;
        private DocumentStore store2;
        private DocumentStore store1;

        [Fact]
        public void Replication_from_specific_collection_should_not_be_valid_failover_destination()
        {
            store1 = CreateStore(databaseName: "FailoverTest");
            store2 = CreateStore(databaseName: "FailoverTest");
            
            store1.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("FailoverTest");
            store2.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("FailoverTest");
            SetupReplication(store1.DatabaseCommands.ForDatabase("FailoverTest"), new Dictionary<string, string> { { "C1s", null } }, store2);

            using (var store = new DocumentStore
            {
                DefaultDatabase = "FailoverTest",
                Url = store1.Url,
                Conventions =
                {
                    FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries
                }
            })
            {
                store.Initialize();
                AsyncHelpers.RunSync(() => ((AsyncServerClient)store.AsyncDatabaseCommands).RequestExecuter.UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store.AsyncDatabaseCommands, true));

                using (var session = store.OpenSession())
                {
                    session.Store(new C1());
                    session.SaveChanges();
                }

                WaitForDocument(store2.DatabaseCommands.ForDatabase("FailoverTest"), "C1s/1");

                servers[0].Dispose();

                //since the replication is collection specific, it is not a valid candidate for failover
                var ex = Assert.Throws<ErrorResponseException>(() =>
                {
                    using (var session = store.OpenSession())
                        session.Load<C1>("C1s/1");
                });

                Assert.Equal(HttpStatusCode.ServiceUnavailable, ex.StatusCode);
            }
        }

        [Fact]
        public void Replication_from_specific_collection_should_not_be_valid_failover_destination2()
        {
            store1 = CreateStore(databaseName: "FailoverTest");
            store2 = CreateStore(databaseName: "FailoverTest");
            store3 = CreateStore(databaseName: "FailoverTest");

            store1.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("FailoverTest");
            store2.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("FailoverTest");
            store3.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("FailoverTest");
            SetupReplication(store1.DatabaseCommands.ForDatabase("FailoverTest"), new Dictionary<string, string> { { "C1s", null } }, store2);
            UpdateReplication(store1.DatabaseCommands.ForDatabase("FailoverTest"), store3);
   
            using (var store = new DocumentStore
            {
                DefaultDatabase = "FailoverTest",
                Url = store1.Url,
                Conventions =
                {
                    FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries
                }
            })
            {
                store.Initialize();

                var replicationInformerForDatabase = store.GetReplicationInformerForDatabase();
                replicationInformerForDatabase.UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store.AsyncDatabaseCommands, true)
                                              .Wait();

                using (var session = store.OpenSession())
                {
                    session.Store(new C1());
                    session.SaveChanges();
                }

                WaitForDocument(store2.DatabaseCommands.ForDatabase("FailoverTest"), "C1s/1");
                WaitForDocument(store3.DatabaseCommands.ForDatabase("FailoverTest"), "C1s/1");

                servers[0].Dispose();

                //since the replication is collection specific, it is not a valid candidate for failover
                //however, here we have store3 that is _not_ collection specific, so it will fetch C1s/1 from store3
                Assert.DoesNotThrow(() =>
                {
                    using (var session = store.OpenSession())
                        session.Load<C1>("C1s/1");
                });
            }
        }

        [Fact]
        public void Replication_from_specified_collections_only_should_work()
        {
            store1 = CreateStore();
            store2 = CreateStore();
            store3 = CreateStore();

            var ids = WriteTracers(store1);

            SetupReplication(store1.DatabaseCommands, new Dictionary<string, string> {{ "C2s", null }}, store2, store3);
            

            Assert.True(WaitForDocument(store2.DatabaseCommands, ids.Item2, 2000));
            Assert.False(WaitForDocument(store2.DatabaseCommands, ids.Item1, 1000));

            Assert.True(WaitForDocument(store3.DatabaseCommands, ids.Item2, 2000));
            Assert.False(WaitForDocument(store3.DatabaseCommands, ids.Item1, 1000));

        }

        private Tuple<string, string> WriteTracers(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var c1 = new C1();
                var c2 = new C2();

                session.Store(c1);
                session.Store(c2);
                session.SaveChanges();

                return Tuple.Create(session.Advanced.GetDocumentId(c1), session.Advanced.GetDocumentId(c2));
            }
        }

        private class C1
        {
        }

        private class C2
        {
        }
    }
}
