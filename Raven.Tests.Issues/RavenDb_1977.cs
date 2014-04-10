using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;
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
                    var res1 = session.SaveChanges();
                    Assert.Contains("object added",res1);
                    var res2 = session.SaveChanges();
                    Assert.True(res2 == null);
                }

                using (var session = store.OpenSession())
                {
                    var userdata1 = session.Load<UserData>("UserData/1");
                    userdata1.ExamMarks[0] = 56;
                    var res11 = session.SaveChanges();
                    string supposed = "field  changed  original type Integer   original value 88 new type Integer new value 56  ";
                    Assert.Equal(supposed, res11);
                    var userdata11 = session.Load<UserData>("UserData/1");
                    int[] newMark = new[] { 67, 65 };
                    var list =userdata1.ExamMarks.ToList();
                    list.AddRange(newMark);
                    userdata1.ExamMarks = list.ToArray();
                    var res12 = session.SaveChanges();
                    string res12_sup = "Field added:  origin  56   , 99   , 77     new 56   , 99   , 77   , 67   , 65      ";
                    Assert.Equal(res12_sup, res12);

                    var userdata22 = session.Load<UserData>("UserData/1");

                    int numToRemove = 99;
                    int numIndex = Array.IndexOf(userdata22.ExamMarks, numToRemove);
                    userdata22.ExamMarks = userdata22.ExamMarks.Where((val, idx) => idx != numIndex).ToArray();
                    var res22 = session.SaveChanges();
                    string res22_sup = "Field removed : origin  56   , 99   , 77   , 67   , 65     new 56   , 77   , 67   , 65      ";
                    Assert.Equal(res22_sup, res22);



                    var userdata = session.Load<UserData>("UserData/3");
                    userdata.Id = 124;
                    userdata.Name = "user3_change";
                    userdata.Salary = 124;
                    var res3 = session.SaveChanges();
                    var supposed_res3 = "field name changed  original type String   original value user3 new type String new value  user3_change ";
                    Assert.Equal(supposed_res3, res3);


                    session.Delete(userdata);
                    var res4 = session.SaveChanges();
                    Assert.Equal("Object UserData/3 deleted", res4);
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
                    var res1 = session.SaveChanges();
                 }

                using (var session = store.OpenSession())
                {
                    var userdata3 = session.Load<UserData>("UserData/3");
                    var metadata3 = session.Advanced.GetMetadataFor(userdata3);
                    metadata3["asdas"] = 1;
                    var res3 = session.SaveChanges();
                    Assert.Contains("field added", res3); 

                    var userdata2 = session.Load<UserData>("UserData/2");
                    var metadata2 = session.Advanced.GetMetadataFor(userdata2);
                    var data2 = metadata2.ElementAt(2);
                    metadata2[data2.Key] = "changes";
                    var res2 = session.SaveChanges();
                    var res2Supposed = "field @etag changed  original type String   original value 01000000-0000-0001-0000-000000000001 new type String new value  changes ";
                    Assert.Equal(res2Supposed, res2);



                    var userdata1 = session.Load<UserData>("UserData/1");
                    var metadata1 = session.Advanced.GetMetadataFor(userdata1);
                    var data1 = metadata1.ElementAt(2);
                    metadata1.Remove(data1.Key);
                    var res1 = session.SaveChanges();

                     Assert.Contains("field removed",res1); 

                   
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
