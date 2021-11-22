using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14753 : RavenTestBase
    {
        public RavenDB_14753(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CountersShouldBeCaseInsensitive()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };

                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("Likes", 999);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.CountersFor(company).Delete("lIkEs");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    var counters = session.CountersFor(company).GetAll();

                    Assert.Equal(0, counters.Count);
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                var countersStorage = database.DocumentsStorage.CountersStorage;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var values = countersStorage
                        .GetCounterValues(context, "companies/1", "Likes")
                        .ToList();

                    Assert.Equal(0, values.Count);
                }
            }
        }
    }
}
