using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_535 : RavenTestBase
    {
        [Fact]
        public async Task CheapGetNextIdentityValueWithoutOverwritingOnExistingDocuments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1337; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }
                var documentDatabase = await GetDocumentDatabaseInstanceFor(store);

                DocumentsOperationContext context;
                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (context.OpenWriteTransaction())
                {
                    var table = context.Transaction.InnerTransaction.OpenTable(DocumentsStorage.DocsSchema, "Collection.Documents.users");
                    int tries;
                    var val = documentDatabase.DocumentsStorage.Identities.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("users/", table, context, out tries);
                    Assert.True(30 > tries);
                    Assert.Equal("users/1338", val);
                    val = documentDatabase.DocumentsStorage.Identities.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("users/", table, context, out tries);
                    Assert.Equal(1, tries);
                    Assert.Equal("users/1339", val);
                }
            }
        }

        private class User { }
    }
}
