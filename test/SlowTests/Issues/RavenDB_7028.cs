using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_7028 : RavenTestBase
    {
        public RavenDB_7028(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DeleteByCollectionShouldOnlyDeleteAllDocsFromThatCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company(), "companies/1");
                    session.Store(new Address(), "addresses/1");

                    session.SaveChanges();
                }

                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfDocuments);

                var operation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = "FROM Companies" }));

                operation.WaitForCompletion(TimeSpan.FromMinutes(5));

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    Assert.Null(company);

                    var address = session.Load<Address>("addresses/1");
                    Assert.NotNull(address);
                }
            }
        }
    }
}
