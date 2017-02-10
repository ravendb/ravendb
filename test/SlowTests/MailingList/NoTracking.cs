// -----------------------------------------------------------------------
//  <copyright file="NoTracking.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.NewClient.Client;
using Xunit;

namespace SlowTests.MailingList
{
    public class NoTracking : RavenNewTestBase
    {
        private static readonly string One = Guid.Parse("00000000-0000-0000-0000-000000000001").ToString();
        private static readonly string Two = Guid.Parse("00000000-0000-0000-0000-000000000002").ToString();

        private IDocumentStore DocumentStore { get; }

        public NoTracking()
        {
            DocumentStore = GetDocumentStore();

            using (var session = DocumentStore.OpenSession())
            {
                var a = new A { Id = One };
                var b = new B { Id = Two };
                a.Bs.Add(Two);

                session.Store(a);
                session.Store(b);
                session.SaveChanges();
            }
        }

        public override void Dispose()
        {
            DocumentStore.Dispose();
            base.Dispose();

        }

        [Fact]
        public void Can_load_entities()
        {
            using (var session = DocumentStore.OpenSession())
            {
                Assert.NotNull(session.Load<A>(One));
                Assert.NotNull(session.Load<B>(Two));
            };
        }

        [Fact]
        public void Can_load_entities_with_NoTracking()
        {
            using (var session = DocumentStore.OpenSession())
            {
                var result = session.Query<A>()
                    .Customize(c => c.NoTracking())
                    .Include<A, B>(a => a.Bs);

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

        [Fact]
        public void Can_load_entities_without_NoTrackin()
        {
            using (var session = DocumentStore.OpenSession())
            {
                var result = session.Query<A>()
                    .Include<A, B>(a => a.Bs);

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
