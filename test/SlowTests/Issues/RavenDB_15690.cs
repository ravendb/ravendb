using System.Threading.Tasks;
using FastTests;
using Orders;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15690 : RavenTestBase
    {
        public RavenDB_15690(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task HasChanges_ShouldDetectDeletes()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.Delete(company);

                    var changes = session.Advanced.WhatChanged();
                    Assert.Equal(1, changes.Count);
                    Assert.True(session.Advanced.HasChanges);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1");
                    session.Delete(company);

                    var changes = session.Advanced.WhatChanged();
                    Assert.Equal(1, changes.Count);
                    Assert.True(session.Advanced.HasChanges);
                }
            }
        }
    }
}
