using System;
using System.Linq;
using System.Net.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bundles.Replication
{
    public class ConflictResolving: ReplicationBase
    {
        [Fact]
        public void ShouldResolveConflictsToLatest()
        {
            ResolveConflictsCore(StraightforwardConflictResolution.ResolveToLatest, (store, id) =>
            {
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(id);
                    Assert.Equal("Remote", company.Name);
                    var metadata = session.Advanced.GetMetadataFor(company);
                   Assert.Equal(2, ReplicationData.GetHistory(metadata).ToList().Count);
                    return true;
                }
            });
        }


        [Fact]
        public void ShouldResolveConflictsToRemote()
        {
            ResolveConflictsCore(StraightforwardConflictResolution.ResolveToRemote, (store,id) =>
            {
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(id);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    Assert.Equal(company.Name,"Remote");
                    Assert.Equal(2, ReplicationData.GetHistory(metadata).ToList().Count);
                    return true;
                }
            });

        }
        [Fact]
        public void ShouldResolveConflictsOnDeletetedToRemoteDelete()
        {
            ResolveConflictsCore(StraightforwardConflictResolution.ResolveToRemote, (store, id) =>
            {
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(id);
                    Assert.Equal(null,company);                   
                    return true;
                }
            }, deleteRemote:true);

        }
        [Fact]
        public void ShouldResolveConflictsOnDeletetedToRemoteDocument()
        {
            ResolveConflictsCore(StraightforwardConflictResolution.ResolveToRemote, (store, id) =>
            {
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(id);
                    Assert.Equal(company.Name, "Remote");
                    var metadata = session.Advanced.GetMetadataFor(company);
                    Assert.Equal(2, ReplicationData.GetHistory(metadata).ToList().Count);

                    return true;
                }
            }, deleteLocal: true);

        }
        [Fact]
        public void ShouldResolveConflictsToLocal()
        {
            ResolveConflictsCore(StraightforwardConflictResolution.ResolveToLocal, (store, id) =>
            {
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(id);
                    Assert.Equal(company.Name, "Local");
                    var metadata = session.Advanced.GetMetadataFor(company);
                    Assert.Equal(2, ReplicationData.GetHistory(metadata).ToList().Count);
                    return true;
                }
            });

        }
        [Fact]
        public void ShouldResolveConflictsOnDeletetedToLocalDelete()
        {
            ResolveConflictsCore(StraightforwardConflictResolution.ResolveToLocal, (store, id) =>
            {
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(id);
                    Assert.Equal(company, null);
                    return true;
                }
            }, deleteLocal : true);

        }
        [Fact]
        public void ShouldResolveConflictsOnDeletetedToLocalDocument()
        {
            ResolveConflictsCore(StraightforwardConflictResolution.ResolveToLocal, (store, id) =>
            {
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(id);
                    Assert.Equal(company.Name, "Local");
                    var metadata = session.Advanced.GetMetadataFor(company);
                    Assert.Equal(2, ReplicationData.GetHistory(metadata).ToList().Count);

                    return true;
                }
            }, deleteRemote: true);

        }
        private void ResolveConflictsCore(StraightforwardConflictResolution resolution, Func<IDocumentStore,string,bool> assertFunc,bool deleteLocal = false, bool deleteRemote = false)
        {
            using (var remote = CreateStore(useFiddler:true))
            using (var local = CreateStore(useFiddler:true))
            {
                TellFirstInstanceToReplicateToSecondInstance();
                string id;
                using (var session = local.OpenSession())
                {
                    var company = new Company {Name = "Local"};
                    session.Store(company);
                    session.SaveChanges();
                    id = session.Advanced.GetDocumentId(company);
                }
                if(deleteLocal)
                {
                    using (var session = local.OpenSession())
                    {
                        session.Delete(id);
                        session.SaveChanges();
                    }
                }
                string remoteId;
                using (var session = remote.OpenSession())
                {
                    var company = new Company {Name = "Remote"};                    
                    session.Store(company);                    
                    session.SaveChanges();
                    remoteId = session.Advanced.GetDocumentId(company);
                }
                if (deleteRemote)
                {
                    using (var session = remote.OpenSession())
                    {
                        session.Delete(remoteId);
                        session.SaveChanges();
                    }
                }
                Assert.True(WaitForConflictDocumentsToAppear(local, id, local.DefaultDatabase),"Waited too long for conflict to be created, giving up.");
                using (var session = local.OpenSession())
                {
                    session.Store(new ReplicationConfig()
                    {
                        DocumentConflictResolution = resolution
                    }, Constants.RavenReplicationConfig);

                    session.SaveChanges();
                }

                //Making sure the conflict index is up and running
                using (var session = local.OpenSession())
                {
                    var res = session.Query<dynamic>(Constants.ConflictDocumentsIndex).Count();

                }
                var requestFactory = new HttpJsonRequestFactory(10);
                var request = requestFactory.CreateHttpJsonRequest(
                    new CreateHttpJsonRequestParams(null, local.Url.ForDatabase(local.DefaultDatabase) + "/replication/forceConflictResolution"
                        , HttpMethod.Get, local.DatabaseCommands.PrimaryCredentials, local.Conventions));
                //Sometimes the conflict index doesn't index fast enough and we would fail if not waiting for indexes
                WaitForIndexing(local);
                request.ExecuteRequest();
                var disapear = WaitForConflictDocumentsToDisappear(local, id, local.DefaultDatabase);
                Assert.True(disapear, $"Waited 15 seconds for conflict to be resolved but there is still a conflict for {id}");
                Assert.True(assertFunc(local,id), "Conflict was resolved but the expected value is wrong");
            }
        }

    }
}
