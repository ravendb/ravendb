using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class Events : RavenTestBase
    {
        public Events(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Before_Store_Listerner()
        {
            using (var store = GetDocumentStore())
            {
                store.OnBeforeStore += eventTest1;
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User()
                    {
                        Name = "Toli",
                        Count = 1
                    } 
                    , "users/1");

                    newSession.Advanced.OnBeforeStore += eventTest2;

                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>("users/1");
                    Assert.Equal(user.Count, 1000);
                    Assert.Equal(user.LastName, "ravendb");
                    user.Age = 3;
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void Before_Store_Session_Listener_With_Load_Inside()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Foo"}, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.OnBeforeStore += delegate(object sender, BeforeStoreEventArgs args)
                    {
                        session.Load<User>("users/1");
                    };

                    session.Store(new User {Name = "Bar"}, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    Assert.NotNull(user1);
                    Assert.Equal("Foo", user1.Name);

                    var user2 = session.Load<User>("users/2");
                    Assert.NotNull(user2);
                    Assert.Equal("Bar", user2.Name);
                }
            }
        }

        [Fact]
        public void Before_Store_Session_Listener_With_Change_Inside()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Foo"}, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.OnBeforeStore += delegate(object sender, BeforeStoreEventArgs args)
                    {
                        var user = session.Load<User>("users/1");
                        user.Name = "Grisha";
                    };

                    session.Store(new User {Name = "Bar"}, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    Assert.NotNull(user1);
                    Assert.Equal("Grisha", user1.Name);

                    var user2 = session.Load<User>("users/2");
                    Assert.NotNull(user2);
                    Assert.Equal("Bar", user2.Name);
                }
            }
        }

        private void eventTest1(object sender, BeforeStoreEventArgs e)
        {
            var user = e.Entity as User;
            if (user != null)
            {
                user.Count = 1000;
            }

            e.DocumentMetadata["Nice"] = "true";
        }

        private void eventTest2(object sender, BeforeStoreEventArgs e)
        {
            var user = e.Entity as User;
            if (user != null)
            {
                user.LastName = "ravendb";
            }
        }
    }
}
