using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12780 : RavenTestBase
    {
        [Fact]
        public void Can_access_id_of_a_missing_loaded_document()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { @"from doc in docs.Users
                        select new{
                            Id1 = Id(LoadDocument(""users/2"", ""users"")),
                            Id2 = Id((string)null).Name
                        }" },
                    Name = "IdIndex"
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }
                WaitForUserToContinueTheTest(store);
                WaitForIndexing(store);

                var stats = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "IdIndex" }));
                Assert.Empty(stats[0].Errors);
            }
        }
    }
}
