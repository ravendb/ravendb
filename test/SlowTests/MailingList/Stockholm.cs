// -----------------------------------------------------------------------
//  <copyright file="Stockholm.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Stockholm : RavenTestBase
    {
        public Stockholm(ITestOutputHelper output) : base(output)
        {
        }

        private class Courses_Search2 : AbstractIndexCreationTask<Course>
        {
            public Courses_Search2()
            {
                Map = courses =>
                      from course in courses
                      select new
                      {
                          Query = new object[]
                        {
                            course.Location,
                            course.Date,
                            course.Technologies,
                            course.Name
                        }
                      };
            }
        }

        private class Course
        {
            public string Name { get; set; }

            public string Instructor { get; set; }

            public string Content { get; set; }

            public string[] Technologies { get; set; }

            public string Location { get; set; }

            public DateTime Date { get; set; }
        }

        [Fact]
        public void ShouldIndexArray()
        {
            using (var store = GetDocumentStore())
            {
                new Courses_Search2().Execute(store);
                using (var session = store.OpenSession())
                {
                    var course = new Course
                    {
                        Name = "RavenDB",
                        Content = new string('*', 10),
                        Date = DateTime.Today,
                        Instructor = "instructors/1",
                        Location = "Stockholm",
                        Technologies = new[] { "dotNet", "RavenDB" }
                    };
                    session.Store(course);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Advanced.DocumentQuery<Course, Courses_Search2>()
                        .WaitForNonStaleResults()
                        .WhereEquals("Query", "dotNet")
                        .ToList());
                }
            }
        }
    }
}
