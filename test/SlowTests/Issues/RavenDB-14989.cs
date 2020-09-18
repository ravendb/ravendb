using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14989 : RavenTestBase
    {
        public RavenDB_14989(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(new string('e', 513), new User {Name = "egor"});
                    var e = await Assert.ThrowsAsync<CompareExchangeKeyTooBigException>(() => session.SaveChangesAsync());
                }
            }
        }
    }
}
