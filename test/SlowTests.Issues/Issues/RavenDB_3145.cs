using FastTests;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3145 : RavenTestBase
    {
        public RavenDB_3145(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var result1 = commands.Put("key/1", null, new { });
                    var result2 = commands.Put("key/1", null, new { });

                    var e = Assert.Throws<ConcurrencyException>(() => commands.Delete("key/1", result1.ChangeVector));
                    Assert.Contains("Optimistic concurrency violation, transaction will be aborted.", e.Message);
                }
            }
        }
    }
}