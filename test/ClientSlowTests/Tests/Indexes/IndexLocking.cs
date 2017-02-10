using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Indexing;
using Raven.Client.Operations.Databases.Indexes;
using Xunit;

namespace NewClientTests.NewClient.Tests.Indexes
{
    public class IndexLocking : RavenNewTestBase
    {
        [Fact]
        public async Task LockingIndexesInMemoryWillNotFail()
        {
            using (var store = GetDocumentStore())
            {
                var index = new IndexSample
                {
                    Conventions = new DocumentConvention()
                };
                index.Execute(store);

                var indexDefinitionOperation = new GetIndexOperation("IndexSample");
                var indexDefinition = store.Admin.Send(indexDefinitionOperation);
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.Unlock);

                var database = await GetDatabase(store.DefaultDatabase);
                database.IndexStore.GetIndex("IndexSample").SetLock(IndexLockMode.LockedIgnore);

                indexDefinitionOperation = new GetIndexOperation("IndexSample");
                indexDefinition = store.Admin.Send(indexDefinitionOperation);
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.LockedIgnore);
            }
        }

        public class IndexSample : AbstractIndexCreationTask<Contact>
        {
            public IndexSample()
            {
                Map = contacts =>
                    from contact in contacts
                    select new
                    {
                        contact.FirstName,
                        PrimaryEmail_EmailAddress = contact.PrimaryEmail.Email,
                    };
            }
        }

        public class Contact
        {
            public string FirstName { get; set; }
            public EmailAddress PrimaryEmail { get; set; }
        }

        public class EmailAddress
        {
            public string Email { get; set; }
        }
    }
}
