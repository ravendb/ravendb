using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class CanLoadDocumentArray : RavenTestBase
    {
        private class Student
        {
            public string Id { get; set; }
            public string Email { get; set; }
            public IEnumerable<string> Friends { get; set; }
        }

        private class Students_WithFriends : AbstractIndexCreationTask<Student, Students_WithFriends.Mapping>
        {
            public class Mapping
            {
                public string EmailDomain { get; set; }
                public IEnumerable<string> Friends { get; set; }
            }

            public Students_WithFriends()
            {
                Map = students => from student in students
                                  let friends = LoadDocument<Student>(student.Friends)
                                  select new Mapping
                                  {
                                      EmailDomain = student.Email.Split('@', StringSplitOptions.None).Last(),
                                      Friends = friends.Select(a => a.Email)
                                  };

                Analyzers.Add(x => x.Friends, "Lucene.Net.Analysis.SimpleAnalyzer, Lucene.Net");
                Indexes.Add(x => x.Friends, FieldIndexing.Search);
            }
        }

        [Fact]
        public void WillSupportLoadDocumentArray()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var student1 = new Student { Email = "support@something.com" };
                    var student2 = new Student { Email = "ayende@something.com" };
                    var student3 = new Student { Email = "oren@something.com" };

                    session.Store(student1);
                    session.Store(student2);
                    student3.Friends = new List<string>() { student1.Id, student2.Id };
                    session.Store(student3);

                    session.SaveChanges();
                }

                new Students_WithFriends().Execute(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Student, Students_WithFriends>()
                                         .Customize(customization => customization.WaitForNonStaleResults())
                                         .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);
                    Assert.Equal(3, results.Count);
                }
            }
        }
    }
}
