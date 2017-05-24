using System;
using System.Diagnostics;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7162 : RavenTestBase
    {
        [Fact]
        public void RequestTimeoutShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "John" });
                    session.SaveChanges();
                }

                using (store.SetRequestsTimeout(TimeSpan.FromMilliseconds(1)))
                {
                    using (var session = store.OpenSession())
                    {
                        var e = Assert.Throws<TimeoutException>(() => session.Query<Person>().ToList());
                        Assert.Contains("failed with timeout after 00:00:00.0010000", e.Message);
                    }
                }
            }
        }
    }
}