using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_3501 : RavenTestBase
    {
        private const int MaxClauseCountInTest = 2048;

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
                                      EmailDomain = student.Email.Split('@').Last(),
                                      Count = 1,
                                  };
            }
        }

        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.MaxClauseCount = MaxClauseCountInTest + 1;
        }

        [Fact]
        public void Too_much_clauses_should_throw_proper_exception()
        {
            using (var server = GetNewServer(port:8090))
            using (var store = NewRemoteDocumentStore(ravenDbServer:server))
            {
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
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var queryString = String.Join(",", list);
                    queryString = string.Format(@"@in<EmailDomain>:({0})", queryString);

                    var query = session.Advanced.DocumentQuery<Student, Students_ByEmailDomain>()
                                                .Where(queryString);

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
            using (var server = GetNewServer(port: 8090))
            using (var store = NewRemoteDocumentStore(ravenDbServer: server))
            {
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
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var queryString = String.Join(",", list);
                    queryString = string.Format(@"@in<EmailDomain>:({0})", queryString);

                    var query = session.Advanced.DocumentQuery<Student, Students_ByEmailDomain>()
                                                .Where(queryString);
                    var value = query.Lazily().Value;

                    Assert.Equal(128, value.Count());
                }
            }
        }
    }
}
