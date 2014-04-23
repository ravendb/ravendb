using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Embedded;
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
                    }, "UserData/1");
                    session.Store(new UserData
                    {
                        Id = 1234,
                        Name = "user2",
                        Salary = 12.51,
                        Date = new DateTime(2014, 1, 1)
                    }, "UserData/2");
                    session.Store(new UserData
                    {
                        Id = 1235,
                        Name = "user3",
                        Salary = 12.45,
                        Date = new DateTime(2014, 1, 2)
                    }, "UserData/3");

                    IDictionary<string, DocumentsChanges[]> resChanges1 = session.Advanced.WhatChanged();
                    int supposedChangesNumber = 3;
                    Assert.Equal(supposedChangesNumber, resChanges1.Count);
                    Assert.True(resChanges1.ContainsKey("UserData/1"));
                    Assert.True(resChanges1.ContainsKey("UserData/2"));
                    Assert.True(resChanges1.ContainsKey("UserData/3"));
                    session.SaveChanges();
                    IDictionary<string, DocumentsChanges[]> resChanges2 = session.Advanced.WhatChanged();
                    supposedChangesNumber = 0;
                    Assert.Equal(supposedChangesNumber, resChanges2.Count);

                    var userdata1 = session.Load<UserData>("UserData/1");
                    int[] newMark = { 67, 65 };
                    List<int> list = userdata1.Exam1Marks.ToList();
                    list.AddRange(newMark);
                    userdata1.Exam1Marks = list.ToArray();
                    IDictionary<string, DocumentsChanges[]> changes4 = session.Advanced.WhatChanged();
                    DocumentsChanges[] data4 = { };
                    if (changes4.TryGetValue("UserData/1", out data4))
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
                    }, "UserData/1");
                    session.Store(new UserData
                    {
                        Id = 1234,
                        Name = "user2",
                        Salary = 12.51,
                        Date = new DateTime(2014, 1, 1)
                    }, "UserData/2");
                    session.Store(new UserData
                    {
                        Id = 1235,
                        Name = "user3",
                        Salary = 12.45,
                        Date = new DateTime(2014, 1, 2)
                    }, "UserData/3");

                    IDictionary<string, DocumentsChanges[]> resChanges1 = session.Advanced.WhatChanged();
                    int supposedChangesNumber = 3;
                    Assert.Equal(supposedChangesNumber, resChanges1.Count);
                    Assert.True(resChanges1.ContainsKey("UserData/1"));
                    Assert.True(resChanges1.ContainsKey("UserData/2"));
                    Assert.True(resChanges1.ContainsKey("UserData/3"));
                    session.SaveChanges();
                    IDictionary<string, DocumentsChanges[]> resChanges2 = session.Advanced.WhatChanged();
                    supposedChangesNumber = 0;
                    Assert.Equal(supposedChangesNumber, resChanges2.Count);
                }

                using (IDocumentSession session = store.OpenSession())
                {
                    var userdata2 = session.Load<UserData>("UserData/2");
                    userdata2.Id = 556;

                    var userdata1 = session.Load<UserData>("UserData/1");
                    userdata1.Exam1Marks[0] = 56;
                    userdata1.Id = 13;
                    userdata1.Salary = 54.7;

                    IDictionary<string, DocumentsChanges[]> changes3 = session.Advanced.WhatChanged();

                    int supposedChangesNumber = 2;
                    Assert.Equal(supposedChangesNumber, changes3.Count);
                    Assert.True(changes3.ContainsKey("UserData/1"));
                    Assert.True(changes3.ContainsKey("UserData/2"));
                    var supposedChanges = new DocumentsChanges
                    {
                        Comment =DocumentsChanges.CommentAsText(DocumentsChanges.CommentType.FieldChanged),
                        FieldName = "Id",
                        FieldNewType = "Integer",
                        FieldNewValue = "556",
                        FieldOldType = "Integer",
                        FieldOldValue = "1234"
                    };
                    DocumentsChanges[] data2 = { };
                    if (changes3.TryGetValue("UserData/2", out data2))
                    {
                        Assert.Equal(data2.Length, 1);
                        Assert.Equal(data2[0], supposedChanges);
                    }

                    DocumentsChanges[] data1 = { };
                    if (changes3.TryGetValue("UserData/1", out data1))
                    {
                        Assert.Equal(data1.Length, 3);
                    }

                    session.SaveChanges();
                    userdata1 = session.Load<UserData>("UserData/1");
                    int[] newMark = { 67, 65 };
                    List<int> list = userdata1.Exam1Marks.ToList();
                    list.AddRange(newMark);
                    userdata1.Exam1Marks = list.ToArray();
                    IDictionary<string, DocumentsChanges[]> changes4 = session.Advanced.WhatChanged();
                    DocumentsChanges[] data4 = { };
                    if (changes4.TryGetValue("UserData/1", out data4))
                    {
                        Assert.Equal(data4.Length, 2);
                    }


                    session.SaveChanges();


                    userdata1 = session.Load<UserData>("UserData/1");
                    int numToRemove = 99;
                    int numIndex = Array.IndexOf(userdata1.Exam1Marks, numToRemove);
                    userdata1.Exam1Marks = userdata1.Exam1Marks.Where((val, idx) => idx != numIndex).ToArray();
                    numToRemove = 77;
                    numIndex = Array.IndexOf(userdata1.Exam1Marks, numToRemove);
                    userdata1.Exam1Marks = userdata1.Exam1Marks.Where((val, idx) => idx != numIndex).ToArray();

                    var userdata = session.Load<UserData>("UserData/3");


                    session.Delete(userdata);
                    session.Store(new UserData
                    {
                        Id = 2235,
                        Name = "user4",
                        Salary = 32.45,
                        Date = new DateTime(2014, 2, 2)
                    }, "UserData/4");


                    IDictionary<string, DocumentsChanges[]> changes7 = session.Advanced.WhatChanged();
                    supposedChangesNumber = 3;
                    Assert.Equal(supposedChangesNumber, changes7.Count);
                    Assert.True(changes7.ContainsKey("UserData/1"));
                    Assert.True(changes7.ContainsKey("UserData/3"));
                    Assert.True(changes7.ContainsKey("UserData/4"));

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
                    }, "UserData/1");
                    session.Store(new UserData
                    {
                        Id = 1234,
                        Name = "user2",
                        Salary = 12.51,
                        Date = new DateTime(2014, 1, 1)
                    }, "UserData/2");
                    session.Store(new UserData
                    {
                        Id = 1235,
                        Name = "user3",
                        Salary = 12.45,
                        Date = new DateTime(2014, 1, 2)
                    }, "UserData/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var userdata2 = session.Load<UserData>("UserData/2");
                    userdata2.Id = 556;

                    var userdata1 = session.Load<UserData>("UserData/1");
                    userdata1.Exam1Marks[0] = 56;
                    userdata1.Exam2Marks[0] = 88;
                    userdata1.Id = 13;
                    userdata1.Salary = 54.7;

                    IDictionary<string, DocumentsChanges[]> changes3 = session.Advanced.WhatChanged();

                    int supposedChangesNumber = 2;
                    Assert.Equal(supposedChangesNumber, changes3.Count);
                    Assert.True(changes3.ContainsKey("UserData/1"));
                    Assert.True(changes3.ContainsKey("UserData/2"));
                    var supposedChanges = new DocumentsChanges
                    {
                        Comment = DocumentsChanges.CommentAsText(DocumentsChanges.CommentType.FieldChanged),
                        FieldName = "Id",
                        FieldNewType = "Integer",
                        FieldNewValue = "556",
                        FieldOldType = "Integer",
                        FieldOldValue = "1234"
                    };
                    DocumentsChanges[] data2 = { };
                    if (changes3.TryGetValue("UserData/2", out data2))
                    {
                        Assert.Equal(data2.Length, 1);
                        Assert.Equal(data2[0], supposedChanges);
                    }

                    DocumentsChanges[] data1 = { };
                    if (changes3.TryGetValue("UserData/1", out data1))
                    {
                        Assert.Equal(data1.Length, 4);
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
                    }, "UserData/1");
                    session.Store(new UserData
                    {
                        Id = 1234,
                        Name = "user2",
                        Salary = 12.51,
                        Date = new DateTime(2014, 1, 1)
                    }, "UserData/2");
                    session.Store(new UserData
                    {
                        Id = 1235,
                        Name = "user3",
                        Salary = 12.45,
                        Date = new DateTime(2014, 1, 2)
                    }, "UserData/3");

                      session.SaveChanges();
                 }

                using (var session = store.OpenSession())
                {
                    var userdata1 = session.Load<UserData>("UserData/1");
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

                    var userdata = session.Load<UserData>("UserData/3");


                    session.Delete(userdata);
    

                    IDictionary<string, DocumentsChanges[]> changes1 = session.Advanced.WhatChanged();
                    var supposedChangesNumber = 2;
                    Assert.Equal(supposedChangesNumber, changes1.Count);
                    Assert.True(changes1.ContainsKey("UserData/1"));
                    Assert.True(changes1.ContainsKey("UserData/3"));

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
                     }, "UserData/1");
                     session.Store(new UserData
                     {
                         Id = 1234,
                         Name = "user2",
                         Salary = 12.51,
                         Date = new DateTime(2014, 1, 1)
                     }, "UserData/2");
                     session.Store(new UserData
                     {
                         Id = 1235,
                         Name = "user3",
                         Salary = 12.45,
                         Date = new DateTime(2014, 1, 2)
                     }, "UserData/3");
                     session.SaveChanges();
                 }

                 using (var session = store.OpenSession())
                 {
                     var userdata3 = session.Load<UserData>("UserData/3");
                     RavenJObject metadata3 = session.Advanced.GetMetadataFor(userdata3);
                     metadata3["tel"] = 1;
                     metadata3["fax"] = 221;
                     metadata3["mail"] = "test_mail";

                     IDictionary<string, DocumentsChanges[]> changes3 = session.Advanced.WhatChanged();
                     int supposedChangesNumber = 1;
                     Assert.Equal(supposedChangesNumber, changes3.Count);
                     Assert.True(changes3.ContainsKey("UserData/3"));
  
                     DocumentsChanges[] data3 = { };
                     if (changes3.TryGetValue("UserData/3", out data3))
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
                     }, "UserData/1");
                     session.Store(new UserData
                     {
                         Id = 1234,
                         Name = "user2",
                         Salary = 12.51,
                         Date = new DateTime(2014, 1, 1)
                     }, "UserData/2");
                     session.Store(new UserData
                     {
                         Id = 1235,
                         Name = "user3",
                         Salary = 12.45,
                         Date = new DateTime(2014, 1, 2)
                     }, "UserData/3");
                     session.SaveChanges();
                 }

                 using (var session = store.OpenSession())
                 {
   
                     var userdata2 = session.Load<UserData>("UserData/2");
                     RavenJObject metadata2 = session.Advanced.GetMetadataFor(userdata2);
                     KeyValuePair<string, RavenJToken> mdata2 = metadata2.ElementAt(2);
                     metadata2[mdata2.Key] = "changes";
                     KeyValuePair<string, RavenJToken> mdata3 = metadata2.ElementAt(3);
                     metadata2[mdata3.Key] = "add changes";
                   
                     int supposedChangesNumber = 1;
                     IDictionary<string, DocumentsChanges[]> changes3 = session.Advanced.WhatChanged();

                     Assert.Equal(supposedChangesNumber, changes3.Count);
                     Assert.True(changes3.ContainsKey("UserData/2"));

                     DocumentsChanges[] data3 = { };
                       if (changes3.TryGetValue("UserData/2", out data3))
                     {
                         Assert.Equal(data3.Length, 2);
                     }
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
                     }, "UserData/1");
                     session.Store(new UserData
                     {
                         Id = 1234,
                         Name = "user2",
                         Salary = 12.51,
                         Date = new DateTime(2014, 1, 1)
                     }, "UserData/2");
                     session.Store(new UserData
                     {
                         Id = 1235,
                         Name = "user3",
                         Salary = 12.45,
                         Date = new DateTime(2014, 1, 2)
                     }, "UserData/3");
                     session.SaveChanges();
                 }

                 using (var session = store.OpenSession())
                 {
  

                     var userdata1 = session.Load<UserData>("UserData/1");
                     RavenJObject metadata1 = session.Advanced.GetMetadataFor(userdata1);

                     KeyValuePair<string, RavenJToken> data1 = metadata1.ElementAt(3);
                     metadata1.Remove(data1.Key);
                     data1 = metadata1.ElementAt(2);
                     metadata1.Remove(data1.Key);
                     IDictionary<string, DocumentsChanges[]> changes3 = session.Advanced.WhatChanged();
                     int supposedChangesNumber = 1;
                     Assert.Equal(supposedChangesNumber, changes3.Count);
                     Assert.True(changes3.ContainsKey("UserData/1"));

                     DocumentsChanges[] data3 = { };
                      if (changes3.TryGetValue("UserData/1", out data3))
                     {
                         Assert.Equal(data3.Length, 2);
                     }
                     session.SaveChanges();
                 }
             }
         }
    }

    internal class UserData
    {
        public DateTime Date;
        public int[] Exam1Marks;
        public int[] Exam2Marks;
        public int Id;
        public string Name;
        public double Salary;
    }
}