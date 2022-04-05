using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_14525 : RavenTestBase
    {
        public RavenDB_14525(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task case_insensitive_collection_name()
        {
            var count = 0;
            var collectionNames = new List<string>
            {
                "Users",
                "users"
            };

            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = x => 
                    x.Conventions = new DocumentConventions
                    {
                        FindCollectionName = x => collectionNames[count++]
                    }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Grisha"}, "users/1");
                    session.Store(new User {Name = "Igal"}, "users/2");
                    session.SaveChanges();
                }

                var documentDatabase = await GetDatabase(store.Database);
                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var collections = context.Transaction.InnerTransaction.OpenTable(DocumentsStorage.CollectionsSchema, Raven.Server.Documents.Schemas.Collections.CollectionsSlice);
                    Assert.Equal(1, collections.NumberOfEntries);
                }
            }
        }
    }
}
