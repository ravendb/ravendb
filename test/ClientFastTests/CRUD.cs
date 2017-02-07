using FastTests;
using Raven.NewClient.Client.Document;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace NewClientTests.NewClient
{
    public class CRUD : RavenNewTestBase
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
                using (var newSession = store.OpenSession())
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
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "user1" }, "users/1");
                    var user2 = new User { Name = "user2", Age = 1 };
                    newSession.Store(user2, "users/2");
                    var user3 = new User { Name = "user3", Age = 1 };
                    newSession.Store(user3, "users/3");
                    newSession.Store(new User { Name = "user4" }, "users/4");

                    newSession.Delete(user2);
                    user3.Age = 3;
                    
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 4);
                    newSession.SaveChanges();

                    var tempUser = newSession.Load<User>("users/2");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/3");
                    Assert.Equal(tempUser.Age, 3);
                    var user1 = newSession.Load<User>("users/1");
                    var user4 = newSession.Load<User>("users/4");

                    newSession.Delete(user4);
                    user1.Age = 10;
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 2);
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
                using (var newSession = store.OpenSession())
                {
                    var family = new Family()
                    {
                        Names = new[] {"Hibernating Rhinos", "RavenDB"}
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<Family>("family/1");
                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    newFamily.Names = new[] {"Toli", "Mitzi", "Boki"};
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Array_In_Object_2()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<Family>("family/1");
                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Array_In_Object_3()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<Family>("family/1");
                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    newFamily.Names = new[] { "RavenDB" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Array_In_Object_4()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<Family>("family/1");
                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos", "Toli", "Mitzi", "Boki" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Array_In_Object_6()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<Family>("family/1");
                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    newFamily.Names = new[] { "RavenDB", "Toli" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Null()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = null }, "users/1");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>("users/1");
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    user.Age = 3;
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                }
            }
        }

        [Fact]
        public void CRUD_Operations_With_Mark_Read_Only()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
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
                using (var newSession = store.OpenSession())
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

        public class FamilyMembers
        {
            public member[] Members { get; set; }
        }
        public class member
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Fact]
        public void CRUD_Operations_With_Array_of_objects()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new FamilyMembers()
                    {
                        Members = new [] {
                            new member()
                            {
                                Name = "Hibernating Rhinos",
                                Age = 8
                            },
                            new member()
                            {
                                Name = "RavenDB",
                                Age = 4
                            }
                        }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<FamilyMembers>("family/1");
                    newFamily.Members = new[]
                    {
                        new member()
                        {
                            Name = "RavenDB",
                            Age = 4
                        },
                        new member()
                        {
                            Name = "Hibernating Rhinos",
                            Age = 8
                        }
                    };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    newFamily.Members = new[]
                    {
                        new member()
                        {
                            Name = "Toli",
                            Age = 5
                        },
                        new member()
                        {
                            Name = "Boki",
                            Age = 15
                        }
                    };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        public class Arr1
        {
            public string[] str { get; set; }
        }

        public class Arr2
        {
            public Arr1[] arr1 { get; set; }
        }

        [Fact]
        public void CRUD_Operations_With_Array_of_Arrays()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var arr = new Arr2()
                    {
                        arr1 = new Arr1[]
                        {
                            new Arr1()
                            {
                                str = new [] {"a", "b"}
                            },
                            new Arr1()
                            {
                                str = new [] {"c", "d"}
                            }
                        } 
                    };
                    newSession.Store(arr, "arr/1");
                    newSession.SaveChanges();

                    var newArr = newSession.Load<Arr2>("arr/1");
                    newArr.arr1 = new Arr1[]
                        {
                            new Arr1()
                            {
                                str = new [] {"d", "c"}
                            },
                            new Arr1()
                            {
                                str = new [] {"a", "b"}
                            }
                       };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    newArr.arr1 = new Arr1[]
                        {
                            new Arr1()
                            {
                                str = new [] {"q", "w"}
                            },
                            new Arr1()
                            {
                                str = new [] {"a", "b"}
                            }
                       };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }
    }
}
