using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class IndexLocking : RavenTestBase
    {
        [Fact]
        public async Task LockingIndexesInMemoryWillNotFail()
        {
            using (var store = await GetDocumentStore())
            {
                var index = new IndexSample
                {
                    Conventions = new DocumentConvention()
                };
                index.Execute(store);

                var indexDefinition = store.DatabaseCommands.GetIndex("IndexSample");
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.Unlock);

                var database = await GetDatabase(store.DefaultDatabase);
                database.IndexStore.GetIndex("IndexSample").SetLock(IndexLockMode.LockedIgnore);

                indexDefinition = store.DatabaseCommands.GetIndex("IndexSample");
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
