using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using FastTests;
using Lucene.Net.Search;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3501 : RavenTestBase
    {
        public RavenDB_3501(ITestOutputHelper output) : base(output)
        {
        }

        private static readonly int InitialMaxClauseCount = BooleanQuery.MaxClauseCount;

        private const int MaxClauseCountInTest = 2048;

        private static int _numberOfTestsToDispose;

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
                                      EmailDomain = student.Email.Split('@', StringSplitOptions.None).Last(),
                                      Count = 1,
                                  };
            }
        }

        [Fact]
        public void Too_much_clauses_should_throw_proper_exception()
        {
            Interlocked.Increment(ref _numberOfTestsToDispose);

            using (var store = GetDocumentStore())
            {
                BooleanQuery.MaxClauseCount = MaxClauseCountInTest;

                var list = new List<string>();
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < MaxClauseCountInTest + 5; i++)
                    {
                        list.Add(i.ToString(CultureInfo.InvariantCulture));
                        session.Store(new Student { Email = "student@" + i });
                    }
                    session.SaveChanges();
                }

                new Students_ByEmailDomain().Execute(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<Student, Students_ByEmailDomain>()
                                                .WhereLucene("EmailDomain", string.Join(" OR ", list.Select(x=> "EmailDomain: " + x)));

                    Assert.Throws<InvalidOperationException>(() => query.Lazily().Value);
                }
            }
        }

        [Theory]
        [InlineData(MaxClauseCountInTest / 4)]
        [InlineData(MaxClauseCountInTest / 2)]
        [InlineData(MaxClauseCountInTest)]
        public void In_Query_should_respect_MaxClauseCount_setting(int numberOfInItems)
        {
            Interlocked.Increment(ref _numberOfTestsToDispose);

            using (var store = GetDocumentStore())
            {
                BooleanQuery.MaxClauseCount = MaxClauseCountInTest;

                var list = new List<string>();
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < numberOfInItems; i++)
                    {
                        list.Add(i.ToString(CultureInfo.InvariantCulture));

                        session.Store(new Student { Email = "student@" + i });
                    }
                    session.SaveChanges();
                }

                new Students_ByEmailDomain().Execute(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<Student, Students_ByEmailDomain>()
                        .WhereIn("EmailDomain", list)
                        .Take(128);

                    var value = query.Lazily().Value;

                    Assert.Equal(128, value.Count());
                }
            }
        }

        public override void Dispose()
        {
            if (Interlocked.Decrement(ref _numberOfTestsToDispose) <= 0)
                BooleanQuery.MaxClauseCount = InitialMaxClauseCount;

            base.Dispose();
        }
    }
}
