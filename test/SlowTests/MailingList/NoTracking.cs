// -----------------------------------------------------------------------
//  <copyright file="NoTracking.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class NoTracking : RavenTestBase
    {
        public NoTracking(ITestOutputHelper output) : base(output)
        {
        }

        private static readonly string One = Guid.Parse("00000000-0000-0000-0000-000000000001").ToString();
        private static readonly string Two = Guid.Parse("00000000-0000-0000-0000-000000000002").ToString();

        private static void CreateData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var a = new A { Id = One };
                var b = new B { Id = Two };
                a.Bs.Add(Two);

                session.Store(a);
                session.Store(b);
                session.SaveChanges();
            }
        }

        [Fact]
        public void Can_load_entities()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Load<A>(One));
                    Assert.NotNull(session.Load<B>(Two));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_load_entities_with_NoTracking(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<A>()
                        .Customize(c => c.NoTracking())
                        .Include(a => a.Bs);

                    foreach (var res in result)
                    {
                        var bs = session.Load<B>(res.Bs);

                        Assert.Equal(bs.Count, 1);
                        // Fails
                        Assert.NotNull(bs.FirstOrDefault());
                    }

                    // Doesn't work either, B is null
                    Assert.NotNull(session.Load<A>(One));
                    Assert.NotNull(session.Load<B>(Two));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_load_entities_without_NoTrackin(Options options)
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<A>()
                        .Include(a => a.Bs);

                    foreach (var res in result)
                    {
                        var bs = session.Load<B>(res.Bs);

                        Assert.Equal(bs.Count, 1);
                        // Fails
                        Assert.NotNull(bs.FirstOrDefault());
                    }

                    Assert.NotNull(session.Load<A>(One));
                    Assert.NotNull(session.Load<B>(Two));
                }
            }
        }

        private class A
        {
            public string Id { get; set; }
            public ISet<string> Bs { get; set; }

            public A()
            {
                Bs = new HashSet<string>();
            }
        }

        private class B
        {
            public string Id { get; set; }
        }
    }
}
