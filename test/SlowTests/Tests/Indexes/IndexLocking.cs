using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
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

                var database = await GetDatabase(store.Database);
                database.IndexStore.GetIndex("IndexSample").SetLock(IndexLockMode.LockedIgnore);

                indexDefinition = store.Admin.Send(new GetIndexOperation("IndexSample"));
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.LockedIgnore);
            }
        }


        [Fact]
        public void set_auto_index_lock_mode()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person()
                    {
                        Name = "Vasia"
                    });
                    session.SaveChanges();
                }
                QueryStatistics statistics;
                using (var session = store.OpenSession())
                {
                    var results = session.Query<Person>().Customize(x => x.WaitForNonStaleResults()).Statistics(out statistics).Where(x => x.Name == "Vasia").ToList();
                }

                store.Admin.Send(new SetIndexLockOperation(statistics.IndexName, IndexLockMode.Unlock));
                var index = store.Admin.Send(new GetIndexOperation(statistics.IndexName));
                Assert.Equal(IndexLockMode.Unlock, index.LockMode);

                store.Admin.Send(new SetIndexLockOperation(statistics.IndexName, IndexLockMode.LockedError));
                index = store.Admin.Send(new GetIndexOperation(statistics.IndexName));
                Assert.Equal(IndexLockMode.LockedError, index.LockMode);

                store.Admin.Send(new SetIndexLockOperation(statistics.IndexName, IndexLockMode.LockedIgnore));
                index = store.Admin.Send(new GetIndexOperation(statistics.IndexName));
                Assert.Equal(IndexLockMode.LockedIgnore, index.LockMode);
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
