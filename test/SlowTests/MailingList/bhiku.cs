// -----------------------------------------------------------------------
//  <copyright file="bhiku.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Xunit;

namespace SlowTests.MailingList
{
    public class Bhiku : RavenTestBase
    {
        [Fact]
        public void CanUseBoost_StartsWith()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Student { FirstName = "David", LastName = "Globe" });
                    session.Store(new Student { FirstName = "Tyson", LastName = "David" });
                    session.Store(new Student { FirstName = "David", LastName = "Jason" });
                    session.SaveChanges();
                }

                new Student_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    var students = session.Advanced.DocumentQuery<Student>()
                        .WaitForNonStaleResults()
                        .WhereStartsWith("FirstName", "David").Boost(3)
                        .WhereStartsWith("LastName", "David")
                        .OrderBy(Constants.Indexing.Fields.IndexFieldScoreName, "LastName")
                        .ToList();

                    Assert.Equal(3, students.Count);

                    Assert.Equal("students/1", students[0].Id);
                    Assert.Equal("students/3", students[1].Id);
                    Assert.Equal("students/2", students[2].Id);
                }
            }
        }

        [Fact]
        public void CanUseBoost_Equal()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Student { FirstName = "David", LastName = "Globe" });
                    session.Store(new Student { FirstName = "Tyson", LastName = "David" });
                    session.Store(new Student { FirstName = "David", LastName = "Jason" });
                    session.SaveChanges();
                }

                new Student_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    var queryable = session.Query<Student, Student_ByName>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == ("David") || x.LastName == ("David"))
                        .OrderByScore().ThenBy(x => x.LastName)
                        ;
                    var students = queryable
                        .ToList();

                    Assert.Equal(3, students.Count);

                    Assert.Equal("students/1", students[0].Id);
                    Assert.Equal("students/3", students[1].Id);
                    Assert.Equal("students/2", students[2].Id);
                }
            }
        }

        private class Student
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public DateTime DateOfBirth { get; set; }
        }

        private class Student_ByName : AbstractIndexCreationTask<Student>
        {
            public Student_ByName()
            {
                Map = students => from s in students
                                  select new
                                  {
                                      FirstName = s.FirstName.Boost(6),
                                      s.LastName,
                                      s.DateOfBirth,
                                  };
            }
        }
    }
}
