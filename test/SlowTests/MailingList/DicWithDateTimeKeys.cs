using System;
using System.Collections.Generic;
using FastTests;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.MailingList
{
    public class DicWithDateTimeKeys : RavenTestBase
    {
        public DicWithDateTimeKeys(ITestOutputHelper output) : base(output)
        {
        }

        private class A
        {
            public IDictionary<DateTimeOffset, string> Items { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void CanSaveAndLoad()
        {
            using (var store = GetDocumentStore())
            {
                var dateTimeOffset = DateTimeOffset.Now;
                using (var session = store.OpenSession())
                {
                    session.Store(new A
                    {
                        Items = new Dictionary<DateTimeOffset, string>
                        {
                            {dateTimeOffset, "a"}
                        }
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var load = session.Load<A>("as/1-A");
                    Assert.Equal("a", load.Items[dateTimeOffset]);
                }
            }
        }
    }
}
