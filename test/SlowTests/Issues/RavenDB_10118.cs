using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10118 : RavenTestBase
    {
        public RavenDB_10118(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldNotThrowAllTopologiesDownException()
        {
            using (var store = GetDocumentStore())
            {
                var session = store.OpenAsyncSession();
                session.Dispose();

                await Assert.ThrowsAsync<ObjectDisposedException>(() => session.Query<User>().ToListAsync());
            }
        }
    }
}
