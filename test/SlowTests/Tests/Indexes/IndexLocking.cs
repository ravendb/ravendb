using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class IndexLocking : RavenTestBase
    {
        [Fact]
        public async Task LockingIndexesInMemoryWillNotFail()
        {
            using (var store = GetDocumentStore())
            {
                var index = new IndexSample
                {
                    Conventions = new DocumentConventions()
                };
                index.Execute(store);

                var indexDefinition = store.Admin.Send(new GetIndexOperation("IndexSample"));
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.Unlock);

                var database = await GetDatabase(store.DefaultDatabase);
                database.IndexStore.GetIndex("IndexSample").SetLock(IndexLockMode.LockedIgnore);

                indexDefinition = store.Admin.Send(new GetIndexOperation("IndexSample"));
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.LockedIgnore);
            }
        }

        private class IndexSample : AbstractIndexCreationTask<Contact>
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

        private class Contact
        {
            public string FirstName { get; set; }
            public EmailAddress PrimaryEmail { get; set; }
        }

        private class EmailAddress
        {
            public string Email { get; set; }
        }
    }
}
