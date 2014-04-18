using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
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
                    
                    var resChanges1 =session.Advanced.WhatChanged();
                    var supposedChanges = new DocumentsChanges 
                    {
                        Comment = "object added: ",
                        DocumentId = "UserData/3",
                        FieldNewType = "Object",
                        FieldOldType = "null", 
                        FieldOldValue = "null", 
                        FieldNewValue = "Name = user3 , Id = 1235 , Salary = 12.45 , Date = 2014-01-02T00:00:00.0000000 , ExamMarks ="};

                    Assert.Equal(supposedChanges, resChanges1);
                    var changes2 = session.Advanced.WhatChanged();
                    supposedChanges.Comment = "Nothing changed";
                    supposedChanges.DocumentId = "";
                    supposedChanges.FieldNewType = "";
                    supposedChanges.FieldNewValue = "";
                    supposedChanges.FieldOldType = "";
                    supposedChanges.FieldOldValue = "";
                    Assert.Equal(changes2, supposedChanges);
                }

                using (var session = store.OpenSession())
                {
                    var userdata1 = session.Load<UserData>("UserData/1");
                    userdata1.ExamMarks[0] = 56;
                    var changes3 = session.Advanced.WhatChanged();
                    var supposedChanges = new DocumentsChanges 
                    {
                        Comment = "field changed",
                        DocumentId = "UserData/1",
                        FieldNewType = "Integer",
                        FieldOldType = "Integer",
                        FieldOldValue = "88", 
                        FieldNewValue = "56"};
                  
                    Assert.Equal(changes3, supposedChanges);

                    var userdata11 = session.Load<UserData>("UserData/1");
                    int[] newMark = new[] { 67, 65 };
                    var list = userdata1.ExamMarks.ToList();
                    list.AddRange(newMark);
                    userdata1.ExamMarks = list.ToArray();
                    var changes4 = session.Advanced.WhatChanged();

                    supposedChanges.Comment = "Field ExamMarks added values: 67   , 65";
                    supposedChanges.DocumentId = "UserData/1";
                    supposedChanges.FieldNewType ="Array";
                    supposedChanges.FieldNewValue = "56   , 99   , 77   , 67   , 65";
                    supposedChanges.FieldOldType = "Array";
                    supposedChanges.FieldOldValue = "56   , 99   , 77";

                    Assert.Equal(changes4, supposedChanges);

   
                    var userdata22 = session.Load<UserData>("UserData/1");

                    int numToRemove = 99;
                    int numIndex = Array.IndexOf(userdata22.ExamMarks, numToRemove);
                    userdata22.ExamMarks = userdata22.ExamMarks.Where((val, idx) => idx != numIndex).ToArray();
                    var changes5 = session.Advanced.WhatChanged();
                                       
                    supposedChanges.Comment = "Field ExamMarks removed values: 99";
                    supposedChanges.DocumentId = "UserData/1";
                    supposedChanges.FieldNewType ="Array";
                    supposedChanges.FieldNewValue ="56   , 77   , 67   , 65";
                    supposedChanges.FieldOldType = "Array";
                    supposedChanges.FieldOldValue = "56   , 99   , 77   , 67   , 65";
                    Assert.Equal(changes5, supposedChanges);

                    var userdata = session.Load<UserData>("UserData/3");
                    userdata.Id = 124;
                    userdata.Name = "user3_change";
                    userdata.Salary = 124;
                     var changes6 = session.Advanced.WhatChanged();
                     supposedChanges.Comment = "field Name changed";
                     supposedChanges.DocumentId = "UserData/3";
                     supposedChanges.FieldNewType = "String";
                     supposedChanges.FieldNewValue = "user3_change";
                     supposedChanges.FieldOldType = "String";
                     supposedChanges.FieldOldValue = "user3";
                     Assert.Equal(changes6, supposedChanges);



                    session.Delete(userdata);
                    var changes7 = session.Advanced.WhatChanged();
  
                    supposedChanges.Comment = "Object deleted";
                     supposedChanges.DocumentId = "UserData/3";
                     supposedChanges.FieldNewType = "";
                     supposedChanges.FieldNewValue = "";
                     supposedChanges.FieldOldType = "";
                     supposedChanges.FieldOldValue = "";
                     Assert.Equal(changes7, supposedChanges);
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
                    metadata3["asdas"] = 1;
                    var changes1 = session.Advanced.WhatChanged();

                     var supposedChanges = new DocumentsChanges 
                    {
                        Comment = "field added: asdas = 1  ",
                        DocumentId = "UserData/3",
                        FieldNewType = "Object",
                        FieldOldType = "Object"};

                     Assert.True(supposedChanges.Comment.Equals(changes1.Comment)&& supposedChanges.DocumentId.Equals(changes1.DocumentId)&&
                         supposedChanges.FieldNewType.Equals(changes1.FieldNewType)&& supposedChanges.FieldOldType.Equals(changes1.FieldOldType));


                    var userdata2 = session.Load<UserData>("UserData/2");
                    var metadata2 = session.Advanced.GetMetadataFor(userdata2);
                    var data2 = metadata2.ElementAt(2);
                     metadata2[data2.Key] = "changes";
                     var changes2 = session.Advanced.WhatChanged();

                    supposedChanges.Comment = "field @etag changed";
                    supposedChanges.DocumentId = "UserData/2";
                    supposedChanges.FieldNewType = "String";
                    supposedChanges.FieldNewValue = "changes";
                    supposedChanges.FieldOldType = "String";
                    Assert.True(supposedChanges.Comment.Equals(changes2.Comment) && supposedChanges.DocumentId.Equals(changes2.DocumentId) &&
                     supposedChanges.FieldNewType.Equals(changes2.FieldNewType) && supposedChanges.FieldOldType.Equals(changes2.FieldOldType) && supposedChanges.FieldNewValue.Equals(changes2.FieldNewValue));

     
                    var userdata1 = session.Load<UserData>("UserData/1");
                    var metadata1 = session.Advanced.GetMetadataFor(userdata1);
                    var data1 = metadata1.ElementAt(2);
                    metadata1.Remove(data1.Key);
                    var changes3 = session.Advanced.WhatChanged();

                    supposedChanges.Comment = "field removed: @etag";
                    supposedChanges.DocumentId = "UserData/1";
                    supposedChanges.FieldNewType = "Object";
                    supposedChanges.FieldOldType = "Object";
                    Assert.True( supposedChanges.DocumentId.Equals(changes3.DocumentId) &&
                     supposedChanges.FieldNewType.Equals(changes3.FieldNewType) && supposedChanges.FieldOldType.Equals(changes3.FieldOldType) );
                    Assert.True(changes3.Comment.Contains(supposedChanges.Comment));


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
