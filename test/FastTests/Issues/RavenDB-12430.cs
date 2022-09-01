using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_12430 : RavenTestBase
    {
        public RavenDB_12430(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void AsExtensionShouldTakeIntoAccountProjection()
        {
            using (var store = GetDocumentStore())
            using (IDocumentSession session = store.OpenSession())
            {
                var withoutAsExtension = session.Query<Order>().ProjectInto<Result>();
                var withAsExtension = session.Query<Order>().ProjectInto<Result>().As<BlittableJsonReaderObject>();

                Assert.True(withAsExtension is IQueryable<BlittableJsonReaderObject>);
                Assert.Equal(withoutAsExtension.ToString(), "from 'Orders' select Company, Employee");
                Assert.Equal(withoutAsExtension.ToString(), withAsExtension.ToString());
            }
        }

        private class Result
        {
            public string Company { get; set; }
            public string Employee { get; set; }
        }
    }
}
