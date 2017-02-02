using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Tests.Indexes
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

                var indexDefinition = store.Admin.Send(new GetIndexOperation("IndexSample"));
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.Unlock);

                var database = await GetDatabase(store.DefaultDatabase);
                database.IndexStore.GetIndex("IndexSample").SetLock(Raven.Abstractions.Indexing.IndexLockMode.LockedIgnore);

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
