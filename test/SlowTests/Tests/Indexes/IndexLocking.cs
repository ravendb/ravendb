using System;
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

                var indexDefinition = store.Maintenance.Send(new GetIndexOperation("IndexSample"));
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.Unlock);

                var database = await GetDatabase(store.Database);
                database.IndexStore.GetIndex("IndexSample").SetLock(IndexLockMode.LockedIgnore);

                indexDefinition = store.Maintenance.Send(new GetIndexOperation("IndexSample"));
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

                var index = store.Maintenance.Send(new GetIndexOperation(statistics.IndexName));
                Assert.Equal(IndexLockMode.Unlock, index.LockMode);
                
                var exception = Assert.Throws<InvalidOperationException>(() => store.Maintenance.Send(new SetIndexesLockOperation(index.Name, IndexLockMode.LockedIgnore)));
                Assert.Equal("'Lock Mode' can't be set for Auto-Indexes.", exception.Message);
                Assert.Equal(IndexLockMode.Unlock, index.LockMode);
                
                exception = Assert.Throws<InvalidOperationException>(() => store.Maintenance.Send(new SetIndexesLockOperation(index.Name, IndexLockMode.LockedError)));
                Assert.Equal("'Lock Mode' can't be set for Auto-Indexes.", exception.Message);
                Assert.Equal(IndexLockMode.Unlock, index.LockMode);
            }
        }
        
        [Fact]
        public async Task SetLockModeForStaticAndAutoIndexes()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person()
                    {
                        Name = "Danielle"
                    });
                    session.SaveChanges();
                }
                
                QueryStatistics statistics;
                
                // create auto-index
                using (var session = store.OpenSession())
                {
                    var results = session.Query<Person>().Customize(x => x.WaitForNonStaleResults()).Statistics(out statistics).Where(x => x.Name == "Danielle").ToList();
                }
                
                // create static-index
                var index = new IndexSample
                {
                    Conventions = new DocumentConventions()
                };
                index.Execute(store);
                
                var indexes = await store.Maintenance.SendAsync(new GetIndexesOperation(0, 128));
                Assert.Equal(2, indexes.Length);

                var autoIndex = indexes[0];
                var staticIndex = indexes[1];
                
                Assert.Equal(IndexLockMode.Unlock, autoIndex.LockMode);
                Assert.Equal(IndexLockMode.Unlock, staticIndex.LockMode);
                
                SetIndexesLockOperation.Parameters indexesParams = new SetIndexesLockOperation.Parameters();
                indexesParams.Mode = IndexLockMode.LockedIgnore;
                indexesParams.IndexNames = new string[] { autoIndex.Name, staticIndex.Name };
                store.Maintenance.Send(new SetIndexesLockOperation(indexesParams));
                
                indexes = await store.Maintenance.SendAsync(new GetIndexesOperation(0, 128));
                autoIndex = indexes[0];
                staticIndex = indexes[1];
                
                Assert.Equal(IndexLockMode.Unlock, autoIndex.LockMode); // auto-index is not expected to change
                Assert.Equal(IndexLockMode.LockedIgnore, staticIndex.LockMode); // static-index is expected to change
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
