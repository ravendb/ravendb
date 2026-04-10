using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22191 : RavenTestBase
    {
        public RavenDB_22191(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Company { get; set; }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void BooleanOperatorsWithDynamicNullObjectShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Users/ByIsActive",
                    Maps =
                    {
                        @"from doc in docs.Users
                          let loadedActive = (LoadDocument(doc.Company, ""Companies"")).IsActive
                          select new {
                              IsActive1 = loadedActive && true,
                              IsActive2 = loadedActive && false,
                              IsActive3 = loadedActive || true,
                              IsActive4 = loadedActive || false,
                              IsActive5 = false || loadedActive,
                              IsActive6 = true && loadedActive
                          }"
                    }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1-A");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Users/ByIsActive" }));
                Assert.Empty(errors[0].Errors);
            }
        }
    }
}
