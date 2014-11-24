// -----------------------------------------------------------------------
//  <copyright file="StartsWith.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
    public class StartsWith : RavenTest
    {
        [Fact]
        public void CanLoadUsingStartsWith()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Id = "customers/1234/users/1" });
                    session.Store(new User { Id = "customers/1234/users/2" });
                    session.Store(new User { Id = "customers/1234/users/3" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loadStartingWith = session.Advanced.LoadStartingWith<User>("customers/1234/users");
                    Assert.Equal(3, loadStartingWith.Count());
                }
            }
        }

        [Fact]
        public void CanLoadUsingStartsWithAndPattern()
        {
            using (var store = NewDocumentStore())
            {
                InsertPatternTestData(store);

                using (var session = store.OpenSession())
                {
                    var loadStartingWith = session.Advanced.LoadStartingWith<User>("customers/1234/", "*/orders");
                    Assert.Equal(2, loadStartingWith.Count());
                }

                using (var session = store.OpenSession())
                {
                    var loadStartingWith = session.Advanced.LoadStartingWith<User>("customers/1234/", "*/orders|*/invoices");
                    Assert.Equal(4, loadStartingWith.Count());
                }
            }
        }

        private static void InsertPatternTestData(EmbeddableDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Id = "customers/1234/users/1" });
                session.Store(new User { Id = "customers/1234/users/1/orders" });
                session.Store(new User { Id = "customers/1234/users/1/invoices" });
                session.Store(new User { Id = "customers/1234/users/2" });
                session.Store(new User { Id = "customers/1234/users/2/orders" });
                session.Store(new User { Id = "customers/1234/users/2/invoices" });
                session.SaveChanges();
            }
        }

        [Fact]
        public void CanGetDocumentsWhoseIdStartsWithAPrefixAndExclusion()
        {
            using (var store = NewDocumentStore())
            {
                InsertPatternTestData(store);

                using (var session = store.OpenSession())
                {
                    var loadStartingWith = session.Advanced.LoadStartingWith<User>("customers/1234/", exclude: "*/invoices");
                    Assert.Equal(4, loadStartingWith.Count());
                }

                using (var session = store.OpenSession())
                {
                    var loadStartingWith = session.Advanced.LoadStartingWith<User>("customers/1234/", exclude: "*/orders");
                    Assert.Equal(4, loadStartingWith.Count());
                }

                using (var session = store.OpenSession())
                {
                    var loadStartingWith = session.Advanced.LoadStartingWith<User>("customers/1234/", exclude: "*/orders|*/invoices");
                    Assert.Equal(2, loadStartingWith.Count());
                }
            }
        }
    }
}