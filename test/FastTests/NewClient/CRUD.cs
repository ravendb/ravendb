using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.NewClient
{
    public class CRUD : RavenTestBase
    {
        protected override void ModifyStore(DocumentStore store)
        {
            store.FailoverServers = null;
        }

        [Fact]
        public void CRUD_Operations()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenNewSession())
                {
                    newSession.Store(new User { Name = "user1" }, "users/1");
                    var user2 = new User {Name = "user2", Age = 1};
                    newSession.Store(user2, "users/2");
                    var user3 = new User { Name = "user3", Age = 1 };
                    newSession.Store(user3, "users/3");
                    newSession.Store(new User { Name = "user4" }, "users/4");
                    
                    newSession.Delete(user2);
                    user3.Age = 3;
                    newSession.SaveChanges();

                    var tempUser = newSession.Load<User>("users/2");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/3");
                    Assert.Equal(tempUser.Age, 3);
                    var user1 = newSession.Load<User>("users/1");
                    var user4 = newSession.Load<User>("users/4");
                    
                    newSession.Delete(user4);
                    user1.Age = 10;
                    newSession.SaveChanges();

                    tempUser = newSession.Load<User>("users/4");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/1");
                    Assert.Equal(tempUser.Age, 10);
                }
            }
        }

        [Fact]
        public void CRUD_Operations_with_what_changed()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenNewSession())
                {
                    newSession.Store(new User { Name = "user1" }, "users/1");
                    var user2 = new User { Name = "user2", Age = 1 };
                    newSession.Store(user2, "users/2");
                    var user3 = new User { Name = "user3", Age = 1 };
                    newSession.Store(user3, "users/3");
                    newSession.Store(new User { Name = "user4" }, "users/4");

                    newSession.Delete(user2);
                    user3.Age = 3;
                    
                    Assert.Equal(newSession.WhatChanged().Count, 4);
                    newSession.SaveChanges();

                    var tempUser = newSession.Load<User>("users/2");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/3");
                    Assert.Equal(tempUser.Age, 3);
                    var user1 = newSession.Load<User>("users/1");
                    var user4 = newSession.Load<User>("users/4");

                    newSession.Delete(user4);
                    user1.Age = 10;
                    Assert.Equal(newSession.WhatChanged().Count, 2);
                    newSession.SaveChanges();

                    tempUser = newSession.Load<User>("users/4");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/1");
                    Assert.Equal(tempUser.Age, 10);
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Array_In_Object()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenNewSession())
                {
                    var family = new Family()
                    {
                        Names = new[] {"Hibernating Rhinos", "RavenDB"}
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<Family>("family/1");
                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos" };
                    Assert.Equal(newSession.WhatChanged().Count, 0);
                    newFamily.Names = new[] {"Toli", "Mitzi", "Boki"};
                    Assert.Equal(newSession.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Null()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenNewSession())
                {
                    newSession.Store(new User { Name = null }, "users/1");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>("users/1");
                    Assert.Equal(newSession.WhatChanged().Count, 0);
                    user.Age = 3;
                    Assert.Equal(newSession.WhatChanged().Count, 1);
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Mark_Read_Only()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenNewSession())
                {
                    newSession.Store(new User { Name = "AAA", Age = 1}, "users/1");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>("users/1");
                    newSession.Advanced.MarkReadOnly(user);
                    user.Age = 2;
                    newSession.SaveChanges();
                    user.Age = 3;
                    newSession.SaveChanges();
                }
                using (var newSession = store.OpenNewSession())
                {
                    var user = newSession.Load<User>("users/1");
                    Assert.Equal(user.Age, 2);
                }
            }
        }

        public class Family
        {
            public string[] Names { get; set; }
        }
    }
}
