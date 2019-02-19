using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class CanCallLastOnArray : RavenTestBase
    {
        private class Student
        {
            public string Email { get; set; }
        }

        private class Students_ByEmailDomain : AbstractIndexCreationTask<Student, Students_ByEmailDomain.Result>
        {
            public class Result
            {
                public string EmailDomain { get; set; }
                public int Count { get; set; }
            }

            public Students_ByEmailDomain()
            {
                Map = students => from student in students
                                  select new
                                  {
                                      EmailDomain = student.Email.Split('@', StringSplitOptions.None).Last(), // does not work
                                      // EmailDomain = student.Email.Split('@')[1],		// DOES WORK
                                      Count = 1
                                  };

                Reduce = results => from result in results
                                    group result by result.EmailDomain
                                    into g
                                    select new
                                    {
                                        EmailDomain = g.Key,
                                        Count = g.Sum(r => r.Count)
                                    };
            }
        }

        [Fact]
        public void WillSupportLast()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Student { Email = "fitzchak@hibernatingrhinos.com" });
                    session.SaveChanges();
                }

                new Students_ByEmailDomain().Execute(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Students_ByEmailDomain.Result, Students_ByEmailDomain>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);
                    Assert.Equal(1, results.Count);
                }
            }
        }

        [Fact]
        public void AssertMapDefinition()
        {
            var indexDefinition = new Students_ByEmailDomain { Conventions = new DocumentConventions { PrettifyGeneratedLinqExpressions = false } }.CreateIndexDefinition();

            Assert.Equal(@"docs.Students.Select(student => new {
    EmailDomain = DynamicEnumerable.LastOrDefault(student.Email.Split('@', System.StringSplitOptions.None)),
    Count = 1
})".Replace("\r\n", Environment.NewLine), indexDefinition.Maps.First());

            Assert.NotEqual(@"docs.Students.Select(student => new {
    EmailDomain = student.Email.Split(new char[] {
        '@'
    }).LastOrDefault(),
    Count = 1
})".Replace("\r\n", Environment.NewLine), indexDefinition.Maps.First());

            Assert.NotEqual(@"docs.Students.Select(student => new {
    EmailDomain = Enumerable.LastOrDefault(student.Email.Split(new char[] {
        '@'
    })),
    Count = 1
})".Replace("\r\n", Environment.NewLine), indexDefinition.Maps.First());

        }
    }
}
