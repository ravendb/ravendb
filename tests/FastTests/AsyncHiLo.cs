using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NewClientTests;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Raven.NewClient.Client.Document;

namespace NewClientTests.NewClient
{
    public class AsyncHiLo : RavenTestBase
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
        public async void Can_Use_Server_Prefix()
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

                    var hiLoKeyGenerator = new AsyncHiLoKeyGenerator("users", store, store.DefaultDatabase,
                        store.Conventions.IdentityPartsSeparator);

                    var generateDocumentKey = await hiLoKeyGenerator.GenerateDocumentKeyAsync(new User());
                    Assert.Equal("users/4,1", generateDocumentKey);
                }
            }
        }

        [Fact]
        public async void Hilo_Cannot_Go_Down()
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

                    var hiLoKeyGenerator = new AsyncHiLoKeyGenerator("users", store, store.DefaultDatabase,
                        store.Conventions.IdentityPartsSeparator);

                    var ids = new HashSet<long> { await hiLoKeyGenerator.NextIdAsync() };

                    hiloDoc.Max = 12;
                    session.Store(hiloDoc, null, "Raven/Hilo/users");
                    session.SaveChanges();

                    for (int i = 0; i < 128; i++)
                    {
                        var nextId = await hiLoKeyGenerator.NextIdAsync();
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
        public async void HiLo_Async_MultiDb()
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


                    var multiDbHiLo = new AsyncMultiDatabaseHiLoKeyGenerator(store, store.Conventions);

                    var generateDocumentKey = await multiDbHiLo.GenerateDocumentKeyAsync(null, new User());
                    Assert.Equal("users/4,65", generateDocumentKey);

                    generateDocumentKey = await multiDbHiLo.GenerateDocumentKeyAsync(null, new Product());
                    Assert.Equal("products/4,129", generateDocumentKey);
                }
            }
        }

        [Fact]
        public async void Capacity_Should_Double()
        {

            using (var store = GetDocumentStore())
            {
                var hiLoKeyGenerator = new AsyncHiLoKeyGenerator("users", store, store.DefaultDatabase,
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
                        await hiLoKeyGenerator.GenerateDocumentKeyAsync(new User());
                    }
;
                }
                using (var session = store.OpenSession())
                {

                    var hiloDoc = session.Load<HiloDoc>("Raven/Hilo/Users");
                    var max = hiloDoc.Max;
                    Assert.Equal(max, 96);

                    await hiLoKeyGenerator.GenerateDocumentKeyAsync(new User()); //we should be receiving a range of 64 now
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
        public async void Return_Unused_Range()
        {

            using (var store = GetDocumentStore())
            {
                var hiLoKeyGenerator = new AsyncHiLoKeyGenerator("users", store, store.DefaultDatabase,
                    store.Conventions.IdentityPartsSeparator);

                using (var session = store.OpenSession())
                {
                    session.Store(new HiloDoc
                    {
                        Max = 64
                    }, "Raven/Hilo/users");

                    session.SaveChanges();

                    await hiLoKeyGenerator.GenerateDocumentKeyAsync(new User());
                    await hiLoKeyGenerator.GenerateDocumentKeyAsync(new User());

                    await hiLoKeyGenerator.ReturnUnusedRangeAsync(); //should change hiloDoc.max to 66                  
                }

                using (var session = store.OpenSession())
                {
                    var hiloDoc = session.Load<HiloDoc>("Raven/Hilo/Users");
                    var max = hiloDoc.Max;
                    Assert.Equal(max, 66);
                }
            }
        }

    }
}
