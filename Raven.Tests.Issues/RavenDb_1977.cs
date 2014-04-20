using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;
using Raven.Tests.Bundles.MoreLikeThis;
using Raven.Tests.Common;

using Xunit;
using Raven.Client.Document;
using System.IO;

namespace Raven.Tests.Issues
{
    public class RavenDb_1977: RavenTest
    {

      
        [Fact]
        public void CanDetectObjectChanges() //add, delete, change 
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
                        ExamMarks = new []{88,99,77} 
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

                    var resChanges1 = session.Advanced.WhatChanged();
                    var supposedChangesNumber = 3;
                    Assert.Equal(supposedChangesNumber, resChanges1.Count);
                    Assert.True(resChanges1.ContainsKey("UserData/1"));
                    Assert.True(resChanges1.ContainsKey("UserData/2"));
                    Assert.True(resChanges1.ContainsKey("UserData/3"));
                   session.SaveChanges();
                  var resChanges2 = session.Advanced.WhatChanged();
                  supposedChangesNumber =0;
                  Assert.Equal(supposedChangesNumber, resChanges2.Count);
              }

                using (var session = store.OpenSession())
                {
                    var userdata2 = session.Load<UserData>("UserData/2");
                    userdata2.Id = 556;

                    var userdata1 = session.Load<UserData>("UserData/1");
                    userdata1.ExamMarks[0] = 56;
                    userdata1.Id = 13;
                    userdata1.Salary = 54.7;
                 
                    var changes3 = session.Advanced.WhatChanged();
                   
                    var supposedChangesNumber =2;
                    Assert.Equal(supposedChangesNumber, changes3.Count);
                    Assert.True(changes3.ContainsKey("UserData/1"));
                    Assert.True(changes3.ContainsKey("UserData/2"));
                    var supposedChanges = new DocumentsChanges()
                    {
                        Comment = "field changed",
                        FieldName = "Id",
                        FieldNewType = "Integer",
                        FieldNewValue = "556",
                        FieldOldType = "Integer",
                        FieldOldValue = "1234"

                    };
                    DocumentsChanges[] data2 = new DocumentsChanges[]{};
                    if (changes3.TryGetValue("UserData/2", out data2))
                    {
                        Assert.Equal(data2.Length, 1);
                        Assert.Equal(data2[0],supposedChanges);
                    }

                    DocumentsChanges[] data1 = new DocumentsChanges[] { };
                    if (changes3.TryGetValue("UserData/1", out data1))
                    {
                        Assert.Equal(data1.Length, 3);
                    }

                    session.SaveChanges();
                    userdata1 = session.Load<UserData>("UserData/1");
                    int[] newMark = new[] { 67, 65 };
                    var list = userdata1.ExamMarks.ToList();
                    list.AddRange(newMark);
                    userdata1.ExamMarks = list.ToArray();
                    var changes4 = session.Advanced.WhatChanged();
                    DocumentsChanges[] data4 = new DocumentsChanges[] { };
                    if (changes4.TryGetValue("UserData/1", out data4))
                    {
                        Assert.Equal(data4.Length, 2);
                    }
                   

                    session.SaveChanges();

     
                    userdata1 = session.Load<UserData>("UserData/1");
                    int numToRemove = 99;
                    int numIndex = Array.IndexOf(userdata1.ExamMarks, numToRemove);
                    userdata1.ExamMarks = userdata1.ExamMarks.Where((val, idx) => idx != numIndex).ToArray();
                    numToRemove = 77;
                     numIndex = Array.IndexOf(userdata1.ExamMarks, numToRemove);
                    userdata1.ExamMarks = userdata1.ExamMarks.Where((val, idx) => idx != numIndex).ToArray();
 
                     var userdata = session.Load<UserData>("UserData/3");
  


                    session.Delete(userdata);
                    session.Store(new UserData
                    {
                        Id = 2235,
                        Name = "user4",
                        Salary = 32.45,
                        Date = new DateTime(2014, 2, 2)
                    }, "UserData/4");


                    var changes7 = session.Advanced.WhatChanged();
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
        public void CanDetectMetadataChanges()
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
                    var metadata3 = session.Advanced.GetMetadataFor(userdata3);
                    metadata3["tel"] = 1;
                    metadata3["fax"] = 221;
                    metadata3["mail"] = 221;

                    var userdata2 = session.Load<UserData>("UserData/2");
                    var metadata2 = session.Advanced.GetMetadataFor(userdata2);
                    var data2 = metadata2.ElementAt(2);
                    metadata2[data2.Key] = "changes";
                    var data3 = metadata2.ElementAt(3);
                    metadata2[data3.Key] = "add changes";
    
     
                    var userdata1 = session.Load<UserData>("UserData/1");
                    var metadata1 = session.Advanced.GetMetadataFor(userdata1);

                    var data1 = metadata1.ElementAt(3);
                    metadata1.Remove(data1.Key);
                    data1 = metadata1.ElementAt(2);
                    metadata1.Remove(data1.Key);
                    var changes3 = session.Advanced.WhatChanged();
                    var supposedChangesNumber = 3;
                    Assert.Equal(supposedChangesNumber, changes3.Count);
                    Assert.True(changes3.ContainsKey("UserData/3"));
                    Assert.True(changes3.ContainsKey("UserData/2"));
                    Assert.True(changes3.ContainsKey("UserData/1"));

                    DocumentsChanges[] data_3 = new DocumentsChanges[] { };
                    if (changes3.TryGetValue("UserData/3", out data_3))
                    {
                        Assert.Equal(data_3.Length, 3);
                    }
                    if (changes3.TryGetValue("UserData/2", out data_3))
                    {
                        Assert.Equal(data_3.Length, 2);
                    }
                    if (changes3.TryGetValue("UserData/1", out data_3))
                    {
                        Assert.Equal(data_3.Length, 2);
                    }
                    session.SaveChanges();
 

                }
            }
        }
       
	

    }

    class UserData
    {
        public string Name;
        public int Id;
        public double Salary;
        public DateTime Date;
        public int[] ExamMarks;


    }
}
