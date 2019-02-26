using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Tests.Infrastructure;
using Xunit;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Tests.Core.Utils.Entities;

namespace SlowTests.Issues
{
    public class RavenDB_12598 : ClusterTestBase
    {
        [Fact]
        public async Task ChangesApiShouldWorkOnAllDatabaseInClusterWhenUsingAggresiveCaching()
        {
            var db = "changes-api-aggressive-caching";
            var docId = "users/1";
            var leader = await CreateRaftClusterAndGetLeader(3);
            await CreateDatabaseInCluster(db, 3, leader.WebUrl);
            IDocumentStore[] stores = new IDocumentStore[3];
            HashSet<string> ensureStoresCommunicateToDiffrentNodes = new HashSet<string>();
            BlockingCollection<DocumentChange>[] blockingCollections = new BlockingCollection<DocumentChange>[3];
            for (var i = 0; i < 3; i++)
            {
                stores[i] = new DocumentStore
                {
                    Urls = new[] { Servers[i].WebUrl },
                    Database = db,
                    Conventions = new DocumentConventions
                    {
                        DisableTopologyUpdates = true
                    }
                    
                }.Initialize();

                Assert.True(ensureStoresCommunicateToDiffrentNodes.Add((await stores[i].GetRequestExecutor(db).GetPreferredNode()).Item2.Url));

                blockingCollections[i] = new BlockingCollection<DocumentChange>();
                var taskObservable = stores[i].Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocument(docId);
                observableWithTask.Subscribe(blockingCollections[i].Add);
                await observableWithTask.EnsureSubscribedNow();
            }

            try
            {
                using (var session = stores[1].OpenSession())
                {
                    session.Store(new User{Name = "Original"}, docId);
                    session.SaveChanges();
                }

                WaitForChangesApiOnAllNodes(blockingCollections,1);
                WaitForDocumentOnAllNodes(stores, docId);
                using(stores[1].AggressivelyCache(db))
                using (var session = stores[1].OpenSession())
                {
                    var user = session.Load<User>(docId);
                    user.Name = "Changed";
                    session.SaveChanges();
                }
                WaitForChangesApiOnAllNodes(blockingCollections, 2);
                AssertValueChangedOnAllNodes(stores, docId);
            }
            finally 
            {
                foreach (var s in stores)
                {
                    s.Dispose();
                }
            }

            
        }

        private void AssertValueChangedOnAllNodes(IDocumentStore[] stores, string docId)
        {
            foreach (var store in stores)
            {
                WaitForValue(() =>
                {
                    using (store.AggressivelyCache(store.Database))
                    using (var session = store.OpenSession())
                    {
                        return session.Load<User>(docId).Name;
                    }
                }, "Changed");
            }
        }

        private void WaitForDocumentOnAllNodes(IDocumentStore[] stores, string docId)
        {
            foreach (var store in stores)
            {
                using (store.AggressivelyCache(store.Database))
                {
                    Assert.True(WaitForDocument(store, docId));
                }
            }
        }

        private void WaitForChangesApiOnAllNodes(BlockingCollection<DocumentChange>[] blockingCollections,int expected)
        {
            for (var i = 0; i < 3; i++)
            {
                WaitForValue(() => blockingCollections[i].Count, expected);
            }
        }
    }
}
