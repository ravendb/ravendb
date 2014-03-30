// -----------------------------------------------------------------------
//  <copyright file="RefreshModifiedDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class RefreshModifiedDocument : Raven.Tests.Helpers.RavenTestBase
    {
        public class Foo
        {
            public string Name { get; set; }
        }

        public abstract class Base
        {
            public Base()
            {
                Foos = Enumerable.Empty<Foo>();
            }

            public IEnumerable<Foo> Foos { get; private set; }

            public void AddFoo(Foo foo)
            {
                Foos = Foos.Concat(new[] { foo });
            }
        }

        public class Derived : Base
        {
            public string Id { get; set; }
        }

        [Fact]
        public void RefreshingEntityDerivedFromAbstractClass()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var d = new Derived();
                    session.Store(d);
                    session.SaveChanges();

                    using (var seperateSession = store.OpenSession())
                    {
                        var loaded = seperateSession.Load<Derived>(d.Id);
                        Assert.NotNull(loaded);
                        Assert.Empty(loaded.Foos);

                        loaded.AddFoo(new Foo() { Name = "a" });
                        seperateSession.SaveChanges();
                    }

                    session.Advanced.Refresh(d);
                    Assert.Single(d.Foos);
                }
            }
        }


        public class NonAbstractBase
        {
            public NonAbstractBase()
            {
                Foos = Enumerable.Empty<Foo>();
            }

            public string Id { get; set; }
            public IEnumerable<Foo> Foos { get; private set; }

            public void AddFoo(Foo foo)
            {
                Foos = Foos.Concat(new[] { foo });
            }
        }

        [Fact]
        public void RefreshingEntityFromClass()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var d = new NonAbstractBase();
                    session.Store(d);
                    session.SaveChanges();

                    using (var seperateSession = store.OpenSession())
                    {
                        var loaded = seperateSession.Load<NonAbstractBase>(d.Id);
                        Assert.NotNull(loaded);
                        Assert.Empty(loaded.Foos);

                        loaded.AddFoo(new Foo() { Name = "a" });
                        seperateSession.SaveChanges();
                    }

                    session.Advanced.Refresh(d);
                    Assert.Single(d.Foos);
                }
            }
        }
    }
}