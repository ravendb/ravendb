using Tests.Infrastructure;
using System;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10717 : RavenTestBase
    {
        public RavenDB_10717(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public DateTime Birthday { get; set; }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanUseDateTimeParametersInQuery(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Birthday = new DateTime(1942, 8, 1)
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var date = new DateTime(1942, 8, 1).AddMinutes(10);
                    var query = from u in session.Query<User>()
                                select new
                                {
                                    Predicate = u.Birthday < date
                                };

                    Assert.Equal("from 'Users' as u select { Predicate : compareDates(u?.Birthday, $p0, 'LessThan') }"
                        , query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.True(result[0].Predicate);
                }
            }
        }
    }
}
