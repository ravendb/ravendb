// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2907.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2907 : RavenTestBase
    {
        public RavenDB_2907(ITestOutputHelper output) : base(output)
        {
        }

        private class Foo
        {
            public string Id { get; set; }
            public Dictionary<string, string> BarIdInKey { get; set; }
            public Dictionary<string, string> BarIdInValue { get; set; }
            protected Foo() { }
            public Foo(Bar[] bars)
            {
                BarIdInKey = bars.ToDictionary(b => b.Id, b => "Some Value");
                BarIdInValue = bars.ToDictionary(b => "Some Key", b => b.Id);
            }
        }

        private class Bar
        {
            public string Id { get; set; }
            public string Title { get; set; }
        }

        private static dynamic MakeAndStoreEntities(IDocumentStore db)
        {
            using (var session = db.OpenSession())
            {
                var bar = new Bar();
                session.Store(bar);
                var foo = new Foo(new[] { bar });
                session.Store(foo);
                session.SaveChanges();
                return new { foo, bar };
            }
        }

        [Fact]
        public void Can_include_dictionary_key()
        {
            using (var db = GetDocumentStore())
            {
                var entities = MakeAndStoreEntities(db);
                var session = db.OpenSession();
                var loaded = session.Include<Foo>(f => f.BarIdInKey.Keys).Load<Foo>(entities.foo.Id);
                Assert.NotNull(loaded);
                Assert.Equal(1, session.Advanced.NumberOfRequests);
                var bar = session.Load<Bar>(entities.bar.Id);
                Assert.NotNull(bar);
                // The following fails because NumberOfRequests is 2
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }

        [Fact]
        public void Can_include_dictionary_value()
        {
            using (var db = GetDocumentStore())
            {
                var entities = MakeAndStoreEntities(db);
                var session = db.OpenSession();
                var foo = session.Include<Foo>(f => f.BarIdInValue.Values.Select(x => x)).Load<Foo>(entities.foo.Id);
                Assert.NotNull(foo);
                Assert.Equal(1, session.Advanced.NumberOfRequests);
                var bar = session.Load<Bar>(entities.bar.Id);
                Assert.NotNull(bar);
                // The following fails because NumberOfRequests is 2
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }
}
