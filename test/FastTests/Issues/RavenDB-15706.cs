using System;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_15706 : RavenTestBase
    {
        public RavenDB_15706(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public void BulkInsertWithoutDB()
        {
            using (var store = new DocumentStore())
            {
                store.Urls = new[] { Server.WebUrl };
                store.Initialize();
                Assert.Throws<InvalidOperationException>(() => store.BulkInsert());
            }
        }
    }
}
