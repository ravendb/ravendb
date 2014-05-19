using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Core.Replication;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.ChangesApi
{
    public class Subscribing : RavenReplicationCoreTest
    {
        [Fact]
        public void CanSubscribeToDocumentChanges()
        {
            using (var store = GetDocumentStore())
            {
                var output = "";

                store.Changes()
                    .ForAllDocuments()
                    .Subscribe(change =>
                    {
                        output = "passed_foralldocuments";
                    });

                store.Changes()
                    .ForDocumentsStartingWith("companies")
                    .Subscribe(change => 
                    {
                        output = "passed_forfordocumentsstartingwith";
                    });

                store.Changes()
                    .ForDocument("companies/1")
                    .Subscribe(change => 
                    {
                        if (change.Type == DocumentChangeTypes.Delete)
                        {
                            output = "passed_fordocumentdelete";
                        }
                    });

                using (var session = store.OpenSession())
                {
                    session.Store(new User 
                    {
                        Id = "users/1"
                    });
                    session.SaveChanges();
                    Assert.Equal("passed_foralldocuments", output);

                    session.Store(new Company
                    {
                        Id = "companies/1"
                    });
                    session.SaveChanges();
                    Assert.Equal("passed_forfordocumentsstartingwith", output);

                    session.Delete("companies/1");
                    session.SaveChanges();
                    Assert.Equal("passed_fordocumentdelete", output);
                }
            }
        }

        [Fact]
        public void CanSubscribeToIndexChanges()
        {
            using (var store = GetDocumentStore())
            {
                var output = "";
                var output2 = "";

                store.Changes()
                    .ForAllIndexes()
                    .Subscribe(change => 
                    {
                        if (change.Type == IndexChangeTypes.IndexAdded)
                        {
                            output = "passed_forallindexesadded";
                        }
                    });

                new Companies_CompanyByType().Execute(store);
                WaitForIndexing(store);
                Assert.Equal("passed_forallindexesadded", output);

                var usersByName = new Users_ByName();
                usersByName.Execute(store);
                WaitForIndexing(store);
                store.Changes()
                    .ForIndex(usersByName.IndexName)
                    .Subscribe(change =>
                    {
                        if (change.Type == IndexChangeTypes.MapCompleted)
                        {
                            output = "passed_forindexmapcompleted";
                        }
                    });

                var companiesSompanyByType = new Companies_CompanyByType();
                companiesSompanyByType.Execute(store);
                WaitForIndexing(store);
                store.Changes()
                    .ForIndex(companiesSompanyByType.IndexName)
                    .Subscribe(change =>
                    {
                        if (change.Type == IndexChangeTypes.RemoveFromIndex)
                        {
                            output2 = "passed_forindexremovecompleted";
                        }
                        if (change.Type == IndexChangeTypes.ReduceCompleted)
                        {
                            output = "passed_forindexreducecompleted";
                        }
                    });

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "user", LastName = "user" });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    Assert.Equal("passed_forindexmapcompleted", output);

                    session.Store(new Company { Id = "companies/1", Name = "company", Type = Company.CompanyType.Public });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    Assert.Equal("passed_forindexreducecompleted", output);

                    session.Delete("companies/1");
                    session.SaveChanges();
                    WaitForIndexing(store);
                    Assert.Equal("passed_forindexremovecompleted", output2);
                }


                store.Changes()
                    .ForAllIndexes()
                    .Subscribe(change =>
                    {
                        if (change.Type == IndexChangeTypes.IndexRemoved)
                        {
                            output = "passed_forallindexesremoved";
                        }
                    });
                store.DatabaseCommands.DeleteIndex("Companies/CompanyByType");
                WaitForIndexing(store);
                Assert.Equal("passed_forallindexesremoved", output);
            }
        }

        [Fact]
        public void CanSubscribeToReplicationConflicts()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                var output = "";

                source.DatabaseCommands.Put("docs/1", null, new RavenJObject() { { "Key", "Value" } }, new RavenJObject());
                destination.DatabaseCommands.Put("docs/1", null, new RavenJObject() { { "Key", "Value" } }, new RavenJObject());

                var eTag = source.DatabaseCommands.Get("docs/1").Etag;

                destination.Changes()
                    .ForAllReplicationConflicts()
                    .Subscribe(conflict =>
                    {
                        output = "conflict";
                    });

                SetupReplication(source, destinations: destination);
                source.Replication.WaitAsync(eTag, replicas: 1).Wait();

                Assert.Equal("conflict", output);
            }
        }
    }
}
