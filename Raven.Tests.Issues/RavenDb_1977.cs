using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDb_1977 : RavenTest
    {
        [Fact]
        public void CanDetectObjectAddChanges()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserData
                    {
                        Id = 123,
                        Name = "user1",
                        Salary = 12.5,
                        Date = DateTime.Now,
                        Exam1Marks = new[] {88, 99, 77}
                    }, "UserDatas/1");
                    session.Store(new UserData
                    {
                        Id = 1234,
                        Name = "user2",
                        Salary = 12.51,
                        Date = new DateTime(2014, 1, 1)
                    }, "UserDatas/2");
                    session.Store(new UserData
                    {
                        Id = 1235,
                        Name = "user3",
                        Salary = 12.45,
                        Date = new DateTime(2014, 1, 2)
                    }, "UserDatas/3");

                    IDictionary<string, DocumentsChanges[]> resChanges1 = session.Advanced.WhatChanged();
                    int supposedChangesNumber = 3;
                    Assert.Equal(supposedChangesNumber, resChanges1.Count);
                    Assert.True(resChanges1.ContainsKey("UserDatas/1"));
                    Assert.True(resChanges1.ContainsKey("UserDatas/2"));
                    Assert.True(resChanges1.ContainsKey("UserDatas/3"));
                    session.SaveChanges();
                    IDictionary<string, DocumentsChanges[]> resChanges2 = session.Advanced.WhatChanged();
                    supposedChangesNumber = 0;
                    Assert.Equal(supposedChangesNumber, resChanges2.Count);

                    var userdata1 = session.Load<UserData>("UserDatas/1");
                    int[] newMark = { 67, 65 };
                    List<int> list = userdata1.Exam1Marks.ToList();
                    list.AddRange(newMark);
                    userdata1.Exam1Marks = list.ToArray();
                    IDictionary<string, DocumentsChanges[]> changes4 = session.Advanced.WhatChanged();
                    DocumentsChanges[] data4 = { };
                    if (changes4.TryGetValue("UserDatas/1", out data4))
                    {
                        Assert.Equal(data4.Length, 2);
                    }


                    session.SaveChanges();

                }

            }
        }
        [Fact]
        public void CanDetectManyObjectChanges() 
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserData
                    {
                        Id = 123,
                        Name = "user1",
                        Salary = 12.5,
                        Date = DateTime.Now,
                        Exam1Marks = new[] { 88, 99, 77 }
                    }, "UserDatas/1");
                    session.Store(new UserData
                    {
                        Id = 1234,
                        Name = "user2",
                        Salary = 12.51,
                        Date = new DateTime(2014, 1, 1)
                    }, "UserDatas/2");
                    session.Store(new UserData
                    {
                        Id = 1235,
                        Name = "user3",
                        Salary = 12.45,
                        Date = new DateTime(2014, 1, 2)
                    }, "UserDatas/3");

                    IDictionary<string, DocumentsChanges[]> resChanges1 = session.Advanced.WhatChanged();
                    int supposedChangesNumber = 3;
                    Assert.Equal(supposedChangesNumber, resChanges1.Count);
                    Assert.True(resChanges1.ContainsKey("UserDatas/1"));
                    Assert.True(resChanges1.ContainsKey("UserDatas/2"));
                    Assert.True(resChanges1.ContainsKey("UserDatas/3"));
                    session.SaveChanges();
                    IDictionary<string, DocumentsChanges[]> resChanges2 = session.Advanced.WhatChanged();
                    supposedChangesNumber = 0;
                    Assert.Equal(supposedChangesNumber, resChanges2.Count);
                }

                using (IDocumentSession session = store.OpenSession())
                {
                    var userdata2 = session.Load<UserData>("UserDatas/2");
                    userdata2.Salary = 556;

                    var userdata1 = session.Load<UserData>("UserDatas/1");
                    userdata1.Exam1Marks[0] = 56;
                    userdata1.Salary = 54.7;

                    IDictionary<string, DocumentsChanges[]> changes3 = session.Advanced.WhatChanged();

                    int ExpectedChangesCount = 2;
                    Assert.Equal(ExpectedChangesCount, changes3.Count);
                    Assert.True(changes3.ContainsKey("UserDatas/1"));
                    Assert.True(changes3.ContainsKey("UserDatas/2"));
                    var supposedChanges = new DocumentsChanges
                    {
                        Change  = DocumentsChanges.ChangeType.FieldChanged,
                        FieldName = "Salary",
                        FieldNewType = "Float",
                        FieldNewValue = "556",
                        FieldOldType = "Float",
                        FieldOldValue = "12.51"
                    };
                    DocumentsChanges[] data2 = { };
                    if (changes3.TryGetValue("UserDatas/2", out data2))
                    {
                        Assert.Equal(data2.Length, 1);
                        Assert.Equal(data2[0], supposedChanges);
                    }

                    DocumentsChanges[] data1 = { };
                    if (changes3.TryGetValue("UserDatas/1", out data1))
                    {
                        Assert.Equal(data1.Length, ExpectedChangesCount); //UserDatas/1 was changed twice
                    }

                    session.SaveChanges();
                    userdata1 = session.Load<UserData>("UserDatas/1");
                    int[] newMark = { 67, 65 };
                    List<int> list = userdata1.Exam1Marks.ToList();
                    list.AddRange(newMark);
                    userdata1.Exam1Marks = list.ToArray();
                    IDictionary<string, DocumentsChanges[]> changes4 = session.Advanced.WhatChanged();
                    DocumentsChanges[] data4 = { };
                    if (changes4.TryGetValue("UserDatas/1", out data4))
                    {
                        Assert.Equal(data4.Length, 2);
                    }


                    session.SaveChanges();


                    userdata1 = session.Load<UserData>("UserDatas/1");
                    int numToRemove = 99;
                    int numIndex = Array.IndexOf(userdata1.Exam1Marks, numToRemove);
                    userdata1.Exam1Marks = userdata1.Exam1Marks.Where((val, idx) => idx != numIndex).ToArray();
                    numToRemove = 77;
                    numIndex = Array.IndexOf(userdata1.Exam1Marks, numToRemove);
                    userdata1.Exam1Marks = userdata1.Exam1Marks.Where((val, idx) => idx != numIndex).ToArray();

                    var userdata = session.Load<UserData>("UserDatas/3");


                    session.Delete(userdata);
                    session.Store(new UserData
                    {
                        Id = 2235,
                        Name = "user4",
                        Salary = 32.45,
                        Date = new DateTime(2014, 2, 2)
                    }, "UserDatas/4");

                    ExpectedChangesCount = 3;
                    IDictionary<string, DocumentsChanges[]> changes7 = session.Advanced.WhatChanged();
                    Assert.Equal(ExpectedChangesCount, changes7.Count);
                    Assert.True(changes7.ContainsKey("UserDatas/1"));
                    Assert.True(changes7.ContainsKey("UserDatas/3"));
                    Assert.True(changes7.ContainsKey("UserDatas/4"));

                    session.SaveChanges();
                }
            }
        }
        [Fact]
        public void CanDetectObjectUpdateChanges() 
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserData
                    {
                        Id = 123,
                        Name = "user1",
                        Salary = 12.5,
                        Date = DateTime.Now,
                        Exam1Marks = new[] { 88, 99, 77 },
                        Exam2Marks = new[] { 77, 78, 79 }
                    }, "UserDatas/1");
                    session.Store(new UserData
                    {
                        Id = 1234,
                        Name = "user2",
                        Salary = 12.51,
                        Date = new DateTime(2014, 1, 1)
                    }, "UserDatas/2");
                    session.Store(new UserData
                    {
                        Id = 1235,
                        Name = "user3",
                        Salary = 12.45,
                        Date = new DateTime(2014, 1, 2)
                    }, "UserDatas/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var userdata2 = session.Load<UserData>("UserDatas/2");
                    userdata2.Salary = 556;

                    var userdata1 = session.Load<UserData>("UserDatas/1");
                    userdata1.Exam1Marks[0] = 56;
                    userdata1.Exam2Marks[0] = 88;
                    userdata1.Salary = 54.7;

                    IDictionary<string, DocumentsChanges[]> changes3 = session.Advanced.WhatChanged();

                    int supposedChangesNumber = 2;
                    Assert.Equal(supposedChangesNumber, changes3.Count);
                    Assert.True(changes3.ContainsKey("UserDatas/1"));
                    Assert.True(changes3.ContainsKey("UserDatas/2"));
                    var supposedChanges = new DocumentsChanges
                    {
                        Change = DocumentsChanges.ChangeType.FieldChanged,
                        FieldName = "Salary",
                        FieldNewType = "Float",
                        FieldNewValue = "556",
                        FieldOldType = "Float",
                        FieldOldValue = "12.51"
                    };
                    DocumentsChanges[] data2;
                    if (changes3.TryGetValue("UserDatas/2", out data2))
                    {
                        Assert.Equal(data2.Length, 1);
                        Assert.Equal(data2[0], supposedChanges);
                    }

                    DocumentsChanges[] data1;
                    if (changes3.TryGetValue("UserDatas/1", out data1))
                    {
                        Assert.Equal(data1.Length, 3);
                    }

                    session.SaveChanges();
  

                    }
            }
        }
         [Fact]
        public void CanDetectObjectDeleteChanges() 
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserData
                    {
                        Id = 123,
                        Name = "user1",
                        Salary = 12.5,
                        Date = DateTime.Now,
                        Exam1Marks = new[] { 88, 99, 77 },
                        Exam2Marks = new[] { 94, 95, 96 }
                    }, "UserDatas/1");
                    session.Store(new UserData
                    {
                        Id = 1234,
                        Name = "user2",
                        Salary = 12.51,
                        Date = new DateTime(2014, 1, 1)
                    }, "UserDatas/2");
                    session.Store(new UserData
                    {
                        Id = 1235,
                        Name = "user3",
                        Salary = 12.45,
                        Date = new DateTime(2014, 1, 2)
                    }, "UserDatas/3");

                      session.SaveChanges();
                 }

                using (var session = store.OpenSession())
                {
                    var userdata1 = session.Load<UserData>("UserDatas/1");
                    int numToRemove = 99;
                    int numIndex = Array.IndexOf(userdata1.Exam1Marks, numToRemove);
                    userdata1.Exam1Marks = userdata1.Exam1Marks.Where((val, idx) => idx != numIndex).ToArray();
                    numToRemove = 77;
                    numIndex = Array.IndexOf(userdata1.Exam1Marks, numToRemove);
                    userdata1.Exam1Marks = userdata1.Exam1Marks.Where((val, idx) => idx != numIndex).ToArray();

                    numToRemove = 96;
                    numIndex = Array.IndexOf(userdata1.Exam2Marks, numToRemove);
                    userdata1.Exam2Marks = userdata1.Exam2Marks.Where((val, idx) => idx != numIndex).ToArray();
                    numToRemove = 95;
                    numIndex = Array.IndexOf(userdata1.Exam2Marks, numToRemove);
                    userdata1.Exam2Marks = userdata1.Exam2Marks.Where((val, idx) => idx != numIndex).ToArray();

                    var userdata = session.Load<UserData>("UserDatas/3");


                    session.Delete(userdata);
    

                    IDictionary<string, DocumentsChanges[]> changes1 = session.Advanced.WhatChanged();
                    var supposedChangesNumber = 2;
                    Assert.Equal(supposedChangesNumber, changes1.Count);
                    Assert.True(changes1.ContainsKey("UserDatas/1"));
                    Assert.True(changes1.ContainsKey("UserDatas/3"));

                    session.SaveChanges();
                }
            }
        }

         [Fact]
         public void CanDetectAddMetadataChanges()
         {
             using (var store = NewDocumentStore())
             {
                 using (var session = store.OpenSession())
                 {
                     session.Store(new UserData
                     {
                         Id = 123,
                         Name = "user1",
                         Salary = 12.5,
                         Date = DateTime.Now
                     }, "UserDatas/1");
                     session.Store(new UserData
                     {
                         Id = 1234,
                         Name = "user2",
                         Salary = 12.51,
                         Date = new DateTime(2014, 1, 1)
                     }, "UserDatas/2");
                     session.Store(new UserData
                     {
                         Id = 1235,
                         Name = "user3",
                         Salary = 12.45,
                         Date = new DateTime(2014, 1, 2)
                     }, "UserDatas/3");
                     session.SaveChanges();
                 }

                 using (var session = store.OpenSession())
                 {
                     var userdata3 = session.Load<UserData>("UserDatas/3");
                     RavenJObject metadata3 = session.Advanced.GetMetadataFor(userdata3);
                     metadata3["tel"] = 1;
                     metadata3["fax"] = 221;
                     metadata3["mail"] = "test_mail";

                     IDictionary<string, DocumentsChanges[]> changes3 = session.Advanced.WhatChanged();
                     int supposedChangesNumber = 1;
                     Assert.Equal(supposedChangesNumber, changes3.Count);
                     Assert.True(changes3.ContainsKey("UserDatas/3"));
  
                     DocumentsChanges[] data3 = { };
                     if (changes3.TryGetValue("UserDatas/3", out data3))
                     {
                         Assert.Equal(data3.Length, 3);
                     }
                         session.SaveChanges();
                 }
             }
         }

        [Fact]
        public void CanDetectUpdateMetadataChanges()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserData
                    {
                        Id = 123,
                        Name = "user1",
                        Salary = 12.5,
                        Date = DateTime.Now
                    }, "UserDatas/1");
                    session.Store(new UserData
                    {
                        Id = 1234,
                        Name = "user2",
                        Salary = 12.51,
                        Date = new DateTime(2014, 1, 1)
                    }, "UserDatas/2");
                    session.Store(new UserData
                    {
                        Id = 1235,
                        Name = "user3",
                        Salary = 12.45,
                        Date = new DateTime(2014, 1, 2)
                    }, "UserDatas/3");
                    session.SaveChanges();
                }

                const string propName1 = "test1";
                const string propName2 = "test2";

                using (var session = store.OpenSession())
                {
                    var userdata1 = session.Load<UserData>("UserDatas/1");
                    RavenJObject metadata = session.Advanced.GetMetadataFor(userdata1);
                    metadata.Add(propName1, null);
                    metadata.Add(propName2, null);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var userdata2 = session.Load<UserData>("UserDatas/2");
                    RavenJObject metadata2 = session.Advanced.GetMetadataFor(userdata2);
                    metadata2[propName1] = "changes";
                    metadata2[propName2] = "add changes";

                    IDictionary<string, DocumentsChanges[]> changes3 = session.Advanced.WhatChanged();
                    Assert.Equal(1, changes3.Count);
                    Assert.True(changes3.ContainsKey("UserDatas/2"));

                    DocumentsChanges[] data3;
                    changes3.TryGetValue("UserDatas/2", out data3);
                    Assert.NotNull(data3);
                    Assert.Equal(2, data3.Length);
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void CanDetectDeleteMetadataChanges()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UserData
                    {
                        Id = 123,
                        Name = "user1",
                        Salary = 12.5,
                        Date = DateTime.Now
                    }, "UserDatas/1");
                    session.Store(new UserData
                    {
                        Id = 1234,
                        Name = "user2",
                        Salary = 12.51,
                        Date = new DateTime(2014, 1, 1)
                    }, "UserDatas/2");
                    session.Store(new UserData
                    {
                        Id = 1235,
                        Name = "user3",
                        Salary = 12.45,
                        Date = new DateTime(2014, 1, 2)
                    }, "UserDatas/3");
                    session.SaveChanges();
                }

                const string propName1 = "test1";
                const string propName2 = "test2";

                using (var session = store.OpenSession())
                {
                    var userdata1 = session.Load<UserData>("UserDatas/1");
                    RavenJObject metadata = session.Advanced.GetMetadataFor(userdata1);
                    metadata.Add(propName1, null);
                    metadata.Add(propName2, null);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var userdata1 = session.Load<UserData>("UserDatas/1");
                    RavenJObject metadata = session.Advanced.GetMetadataFor(userdata1);

                    RavenJToken value;
                    metadata.TryGetValue(propName1, out value);
                    Assert.NotNull(value);
                    metadata.TryGetValue(propName2, out value);
                    Assert.NotNull(value);

                    metadata.Remove(propName1);
                    metadata.Remove(propName2);

                    IDictionary<string, DocumentsChanges[]> changes3 = session.Advanced.WhatChanged();
                    Assert.Equal(1, changes3.Count);
                    Assert.True(changes3.ContainsKey("UserDatas/1"));

                    DocumentsChanges[] data3;
                    changes3.TryGetValue("UserDatas/1", out data3);
                    Assert.NotNull(data3);
                    Assert.Equal(2, data3.Length);
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void CanDetectChangesInNestedObject()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Friend
                    {
                        UserData = new UserData
                        {
                            Id = 123,
                            Name = "user1",
                            Salary = 12.5,
                            Exam1Marks = new []{1,2,},
                            Date = DateTime.Now
                        }
                    }, "friends/1");
                    
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var friend = session.Load<Friend>(1);

                    friend.UserData.Name = "Foo";
                    friend.UserData.Exam1Marks = new[] {1, 2, 3};

                    var changes3 = session.Advanced.WhatChanged();

                    Assert.Equal("UserData.Name", changes3["friends/1"][0].FieldName);
                    Assert.Equal("UserData.Exam1Marks[2]", changes3["friends/1"][1].FieldName);
                }
            }
        }

        [Fact]
        public void CanDetectBigChangesInNestedObject()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new BigFriend
                    {
                        Descr="Test",
                        UserData = new UserData
                        {
                            Id = 123,
                            Name = "user1",
                            Salary = 12.5,
                            Exam1Marks = new[] { 1, 2, },
                            Date = DateTime.Now
                        }
                    }, "bigfriends/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var bigFriend = session.Load<BigFriend>(1);
                    bigFriend.Descr = "New descr";
                    bigFriend.UserData.Name = "Foo";
                    bigFriend.UserData.Exam1Marks = new[] { 1, 2, 3 };

                    var changes3 = session.Advanced.WhatChanged();

                    Assert.Equal("Descr", changes3["bigfriends/1"][0].FieldName);
                    Assert.Equal("UserData.Name", changes3["bigfriends/1"][1].FieldName);
                    Assert.Equal("UserData.Exam1Marks[2]", changes3["bigfriends/1"][2].FieldName);
                }
            }
        }

        public class UserData
        {
            public DateTime Date;
            public int[] Exam1Marks;
            public int[] Exam2Marks;
            public int Id;
            public string Name;
            public double Salary;
        }

        public class Friend
        {
            public UserData UserData { get; set; }
        }
        public class BigFriend
        {
            public UserData UserData { get; set; }
            public string Descr;
        }
    }

  
}
