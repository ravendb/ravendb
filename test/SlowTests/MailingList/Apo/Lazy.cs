using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Xunit;

namespace SlowTests.MailingList.Apo
{
    public class Lazy : RavenTestBase
    {
        private class TestClass
        {
            public string Id { get; set; }

            public string Value { get; set; }

            public DateTime Date { get; set; }
        }

        [Fact]
        public async Task LazyWhereAndOrderBy()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestClass() { Id = "testid", Value = "test1", Date = DateTime.UtcNow });
                    session.Store(new TestClass() { Value = "test2", Date = DateTime.UtcNow });
                    session.Store(new TestClass() { Value = "test3", Date = DateTime.UtcNow.AddMinutes(1) });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var hello = new List<TestClass>();

                    // should not throw
                    session.Query<TestClass>()
                            .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
                            .Where(x => x.Date >= DateTime.UtcNow.AddMinutes(-1))
                            .OrderByDescending(x => x.Date)
                            .Lazily(result =>
                            {
                                hello = result.ToList();
                            });
                }
            }
        }
    }
}
