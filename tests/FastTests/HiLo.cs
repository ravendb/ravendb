using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Raven.NewClient.Client.Document;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;


namespace NewClientTests.NewClient
{
    public class Hilo : RavenTestBase 
    {
        private class HiloDoc
        {
            public long Max { get; set; }
        }

        private class PrefixHiloDoc
        {
            public string ServerPrefix { get; set; }
        }

        private class Product
        {
            public string ProductName { get; set; }
        }

        [Fact]
        public void Can_Use_Server_Prefix()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {                    
                    session.Store(new PrefixHiloDoc()
                    {
                        ServerPrefix = "4,"
                    }, "Raven/ServerPrefixForHilo");

                    session.SaveChanges();

                    var hiLoKeyGenerator = new HiLoKeyGenerator("users", store, store.DefaultDatabase,
                        store.Conventions.IdentityPartsSeparator);

                    var generateDocumentKey = hiLoKeyGenerator.GenerateDocumentKey(new User());
                    Assert.Equal("users/4,1", generateDocumentKey);
                }
            }
        }

        [Fact]
        public void Hilo_Cannot_Go_Down()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var hiloDoc = new HiloDoc
                    {
                        Max = 32
                    };
                    session.Store(hiloDoc, "Raven/Hilo/users");
                    session.SaveChanges();

                    var hiLoKeyGenerator = new HiLoKeyGenerator("users", store, store.DefaultDatabase,
                        store.Conventions.IdentityPartsSeparator);

                    var ids = new HashSet<long> {hiLoKeyGenerator.NextId()};

                    hiloDoc.Max = 12;
                    session.Store(hiloDoc, null, "Raven/Hilo/users");
                    session.SaveChanges();

                    for (int i = 0; i < 128; i++)
                    {
                        var nextId = hiLoKeyGenerator.NextId();
                        Assert.True(ids.Add(nextId), "Failed at " + i);
                    }

                    var list = ids.GroupBy(x => x).Select(g => new
                    {
                        g.Key,
                        Count = g.Count()
                    }).Where(x => x.Count > 1).ToList();

                    Assert.Empty(list);
                }
            }
        }

        [Fact]
        public void HiLo_MultiDb()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new HiloDoc
                    {
                        Max = 64
                    }, "Raven/Hilo/users");

                    session.Store(new HiloDoc
                    {
                        Max = 128
                    }, "Raven/Hilo/products");

                    session.Store(new PrefixHiloDoc()
                    {
                        ServerPrefix = "4,"
                    }, "Raven/ServerPrefixForHilo");

                    session.SaveChanges();
                    
                    var multiDbHiLo = new MultiDatabaseHiLoGenerator(store, store.Conventions);

                    var generateDocumentKey = multiDbHiLo.GenerateDocumentKey(null,new User());
                    Assert.Equal("users/4,65", generateDocumentKey);

                    generateDocumentKey = multiDbHiLo.GenerateDocumentKey(null, new Product());
                    Assert.Equal("products/4,129", generateDocumentKey);
                }
            }
        }

        [Fact]
        public void Capacity_Should_Double()
        {
            using (var store = GetDocumentStore())
            {
                var hiLoKeyGenerator = new HiLoKeyGenerator("users", store, store.DefaultDatabase,
                    store.Conventions.IdentityPartsSeparator);

                using (var session = store.OpenSession())
                {
                    session.Store(new HiloDoc
                    {
                        Max = 64
                    }, "Raven/Hilo/users");

                    session.SaveChanges();

                    for (int i = 0; i < 32; i++)
                    {
                        hiLoKeyGenerator.GenerateDocumentKey(new User());
                    }
                }
                using (var session = store.OpenSession())
                {

                    var hiloDoc = session.Load<HiloDoc>("Raven/Hilo/users");
                    var max = hiloDoc.Max;
                    Assert.Equal(max, 96);

                    hiLoKeyGenerator.GenerateDocumentKey(new User()); //we should be receiving a range of 64 now
                }

                using (var session = store.OpenSession())
                { 
                    var hiloDoc = session.Load<HiloDoc>("Raven/Hilo/users");
                    var max = hiloDoc.Max;
                    Assert.Equal(max, 160);
                }
            }
        }

        [Fact]
        public void Return_Unused_Range_On_Dispose()
        {
            using (var store = GetDocumentStore())
            {
                var newStore = new DocumentStore()
                {
                    Url = store.Url,
                    DefaultDatabase = store.DefaultDatabase,
                    ApiKey = store.ApiKey
                };
                newStore.Initialize();

                using (var session = newStore.OpenSession())
                {
                    session.Store(new HiloDoc
                    {
                        Max = 32
                    }, "Raven/Hilo/users");

                    session.SaveChanges();

                    session.Store(new User());
                    session.Store(new User());

                    session.SaveChanges();
                }
                newStore.Dispose(); //on document store dispose, hilo-return should be called 

                newStore = new DocumentStore()
                {
                    Url = store.Url,
                    DefaultDatabase = store.DefaultDatabase,
                    ApiKey = store.ApiKey
                };
                newStore.Initialize();

                using (var session = newStore.OpenSession())
                {
                    var hiloDoc = session.Load<HiloDoc>("Raven/Hilo/users");
                    var max = hiloDoc.Max;
                    Assert.Equal(max, 34);
                }
                newStore.Dispose();
            } 
        }

        [Fact]
        public void Should_Resolve_Conflict_With_Highest_Number()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "foo1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "foo2"))
            {
                using (var s1 = store1.OpenSession())
                {
                    var hiloDoc = new HiloDoc
                    {
                        Max = 128
                    };
                    s1.Store(hiloDoc, "Raven/Hilo/users");
                    s1.Store(new User(), "marker/doc");
                    s1.SaveChanges();
                }
                using (var s2 = store2.OpenSession())
                {
                    var hiloDoc2 = new HiloDoc
                    {
                        Max = 64
                    };
                    s2.Store(hiloDoc2, "Raven/Hilo/users");
                    s2.SaveChanges();    
                }

                SetupReplication(store1, store2);

                WaitForMarkerDocumentAndAllPrecedingDocumentsToReplicate(store2);
                
                var nextId = new HiLoKeyGenerator("users", store2, store2.DefaultDatabase,
                    store2.Conventions.IdentityPartsSeparator).NextId();
                Assert.Equal(nextId, 129);                              
            }
        }

        private static void WaitForMarkerDocumentAndAllPrecedingDocumentsToReplicate(DocumentStore store2)
        {
            var sp = Stopwatch.StartNew();
            while (true)
            {
                using (var session = store2.OpenSession())
                {
                    if (session.Load<object>("marker/doc") != null)
                        break;
                    Thread.Sleep(32);
                }
                if (sp.Elapsed.TotalSeconds > 30)
                    throw new TimeoutException("waited too long");
            }
        }

        protected static void SetupReplication(DocumentStore fromStore, 
             DocumentStore toStore)
        {
            using (var session = fromStore.OpenSession())
            {
                session.Store(new ReplicationDocument
                {
                    Destinations = new List<ReplicationDestination>
                    {
                        new ReplicationDestination
                        {
                            Database = toStore.DefaultDatabase,
                            Url = toStore.Url,
                        }
                    },
                    DocumentConflictResolution = StraightforwardConflictResolution.None
                }, Constants.Replication.DocumentReplicationConfiguration);
                session.SaveChanges();
            }
        }
    }
}
