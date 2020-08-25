using System;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15512 : RavenTestBase
    {
        public RavenDB_15512(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldNotThrowOnWriteAssurance()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 13 /* intentionally setting to high value*/, timeout: TimeSpan.FromMilliseconds(1), throwOnTimeout: false);

                    session.Store(new User(), "users/1");

                    session.SaveChanges();
                }
            }
        }
    }
}
