using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
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
        public void LazyWhereAndOrderBy()
        {
            using (var store = GetDocumentStore())
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
                            .Customize(x => x.WaitForNonStaleResults())
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
