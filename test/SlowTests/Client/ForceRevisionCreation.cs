using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class ForceRevisionCreation : RavenTestBase
    {
        public ForceRevisionCreation(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ForceRevisionCreationForSingleUnTrackedEntityByID()
        {
            using (var store = GetDocumentStore())
            {
                string companyId;
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    companyId = company.Id;
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Revisions.ForceRevisionCreationFor(companyId);
                    session.SaveChanges();

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(companyId).Count;
                    Assert.Equal(1, revisionsCount);
                }
            }
        }

        [Fact]
        public async Task ForceRevisionCreationForSingleUnTrackedEntityByID_Async()
        {
            using (var store = GetDocumentStore())
            {
                string companyId;
                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company { Name = "HR" };
                    await session.StoreAsync(company);
                    companyId = company.Id;
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Revisions.ForceRevisionCreationFor(companyId);
                    await session.SaveChangesAsync();

                    var revisionsCount = (await session.Advanced.Revisions.GetForAsync<Company>(companyId)).Count;
                    Assert.Equal(1, revisionsCount);
                }
            }
        }

        [Fact]
        public void ForceRevisionCreationForMultipleUnTrackedEntitiesByID()
        {
            using (var store = GetDocumentStore())
            {
                string companyId1;
                string companyId2;

                using (var session = store.OpenSession())
                {
                    var company1 = new Company { Name = "HR1" };
                    var company2 = new Company { Name = "HR2" };

                    session.Store(company1);
                    session.Store(company2);

                    companyId1 = company1.Id;
                    companyId2 = company2.Id;

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Revisions.ForceRevisionCreationFor(companyId1);
                    session.Advanced.Revisions.ForceRevisionCreationFor(companyId2);

                    session.SaveChanges();

                    var revisionsCount1 = session.Advanced.Revisions.GetFor<Company>(companyId1).Count;
                    var revisionsCount2 = session.Advanced.Revisions.GetFor<Company>(companyId2).Count;

                    Assert.Equal(1, revisionsCount1);
                    Assert.Equal(1, revisionsCount2);
                }
            }
        }

        [Fact]
        public void CannotForceRevisionCreationForUnTrackedEntityByEntity()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };

                    var ex = Assert.Throws<InvalidOperationException>(() => session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company));
                    Assert.Contains("Cannot create a revision for the requested entity because it is Not tracked by the session", ex.Message);
                }
            }
        }

        [Fact]
        public void ForceRevisionCreationForNewDocumentByEntity()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    session.SaveChanges();

                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);

                    session.SaveChanges();

                    revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(1, revisionsCount);
                }
            }
        }

        [Fact]
        public void CannotForceRevisionCreationForNewDocumentBeforeSavingToServerByEntity()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);

                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);

                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Contains("Can't force revision creation - the document was not saved on the server yet", ex.Message);

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);
                }
            }
        }

        [Fact]
        public void ForceRevisionCreationForTrackedEntityWithNoChangesByEntity()
        {
            using (var store = GetDocumentStore())
            {
                var companyId = "";

                using (var session = store.OpenSession())
                {
                    // 1. Store document
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    session.SaveChanges();
                    companyId = company.Id;

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);
                }

                using (var session = store.OpenSession())
                {
                    // 2. Load & Save without making changes to the document
                    var company = session.Load<Company>(companyId);

                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(companyId).Count;
                    Assert.Equal(1, revisionsCount);
                }
            }
        }

        [Fact]
        public void ForceRevisionCreationForTrackedEntityWithChangesByEntity()
        {
            using (var store = GetDocumentStore())
            {
                var companyId = "";

                // 1. Store document
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    session.SaveChanges();
                    companyId = company.Id;

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);
                }

                using (var session = store.OpenSession())
                {
                    // 2. Load, Make changes & Save
                    var company = session.Load<Company>(companyId);
                    company.Name = "HR V2";

                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    List<Company> revisions = session.Advanced.Revisions.GetFor<Company>(company.Id);
                    var revisionsCount = revisions.Count;

                    Assert.Equal(1, revisionsCount);
                    // Assert revision contains the value 'Before' the changes...
                    // ('Before' is the default force revision creation strategy)
                    Assert.Equal("HR", revisions[0].Name);
                }
            }
        }

        [Fact]
        public void ForceRevisionCreationForTrackedEntityWithChangesByID()
        {
            using (var store = GetDocumentStore())
            {
                var companyId = "";

                // 1. Store document
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    session.SaveChanges();
                    companyId = company.Id;

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);
                }

                using (var session = store.OpenSession())
                {
                    // 2. Load, Make changes & Save
                    var company = session.Load<Company>(companyId);
                    company.Name = "HR V2";

                    session.Advanced.Revisions.ForceRevisionCreationFor(company.Id);
                    session.SaveChanges();

                    List<Company> revisions = session.Advanced.Revisions.GetFor<Company>(company.Id);
                    var revisionsCount = revisions.Count;

                    Assert.Equal(1, revisionsCount);
                    // Assert revision contains the value 'Before' the changes...
                    Assert.Equal("HR", revisions[0].Name);
                }
            }
        }

        [Fact]
        public void ForceRevisionCreationMultipleRequests()
        {
            using (var store = GetDocumentStore())
            {
                var companyId = "";

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    session.SaveChanges();
                    companyId = company.Id;

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Revisions.ForceRevisionCreationFor(companyId);

                    var company = session.Load<Company>(companyId);
                    company.Name = "HR V2";

                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    // The above request should not throw - we ignore duplicate requests with SAME strategy

                    var ex = Assert.Throws<InvalidOperationException>(() => session.Advanced.Revisions.ForceRevisionCreationFor(company.Id, ForceRevisionStrategy.None));
                    // the above should throw because we ask for different strategy in the same session
                    Assert.Contains("A request for creating a revision was already made for document", ex.Message);

                    session.SaveChanges();

                    List<Company> revisions = session.Advanced.Revisions.GetFor<Company>(company.Id);
                    var revisionsCount = revisions.Count;

                    Assert.Equal(1, revisionsCount);
                    Assert.Equal("HR", revisions[0].Name);
                }
            }
        }

        [Fact]
        public void ForceRevisionCreationAcrossMultipleSessions()
        {
            using (var store = GetDocumentStore())
            {
                var companyId = "";

                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };
                    session.Store(company);
                    session.SaveChanges();

                    companyId = company.Id;
                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);

                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(1, revisionsCount);

                    // Verify that another 'force' request will not create another revision
                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(1, revisionsCount);
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(companyId);
                    company.Name = "HR V2";

                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    List<Company> revisions = session.Advanced.Revisions.GetFor<Company>(company.Id);
                    var revisionsCount = revisions.Count;

                    Assert.Equal(1, revisionsCount);
                    // Assert revision contains the value 'Before' the changes...
                    Assert.Equal("HR", revisions[0].Name);

                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    revisions = session.Advanced.Revisions.GetFor<Company>(company.Id);
                    revisionsCount = revisions.Count;

                    Assert.Equal(2, revisionsCount);
                    // Assert revision contains the value 'Before' the changes...
                    Assert.Equal("HR V2", revisions[0].Name);
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(companyId);
                    company.Name = "HR V3";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Revisions.ForceRevisionCreationFor(companyId);
                    session.SaveChanges();

                    List<Company> revisions = session.Advanced.Revisions.GetFor<Company>(companyId);
                    var revisionsCount = revisions.Count;

                    Assert.Equal(3, revisionsCount);
                    Assert.Equal("HR V3", revisions[0].Name);
                }
            }
        }

        [Fact]
        public async Task ForceRevisionCreationWhenRevisionConfigurationIsSet()
        {
            using (var store = GetDocumentStore())
            {
                // Define revisions settings
                var configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        {
                            "Companies", new RevisionsCollectionConfiguration
                            {
                                PurgeOnDelete = true,
                                MinimumRevisionsToKeep = 5
                             }
                        }
                    }
                };

                var result = await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));
                var companyId = "";

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    companyId = company.Id;
                    session.SaveChanges();

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(1, revisionsCount); // one revision because configuration is set

                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(1, revisionsCount); // no new revision created - already exists due to configuration settings

                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    company.Name = "HR V2";
                    session.SaveChanges();

                    List<Company> revisions = session.Advanced.Revisions.GetFor<Company>(companyId);
                    revisionsCount = revisions.Count;

                    Assert.Equal(2, revisionsCount);
                    Assert.Equal("HR V2", revisions[0].Name);
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Revisions.ForceRevisionCreationFor(companyId);
                    session.SaveChanges();

                    List<Company> revisions = session.Advanced.Revisions.GetFor<Company>(companyId);
                    var revisionsCount = revisions.Count;

                    Assert.Equal(2, revisionsCount);
                    Assert.Equal("HR V2", revisions[0].Name);
                }
            }
        }

        [Fact]
        public void HasRevisionsFlagIsCreatedWhenForcingRevisionForDocumentThatHasNoRevisionsYet()
        {
            using (var store = GetDocumentStore())
            {
                var company1Id = "";
                var company2Id = "";

                using (var session = store.OpenSession())
                {
                    var company1 = new Company { Name = "HR1" };
                    var company2 = new Company { Name = "HR2" };

                    session.Store(company1);
                    session.Store(company2);

                    session.SaveChanges();

                    company1Id = company1.Id;
                    company2Id = company2.Id;

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company1.Id).Count;
                    Assert.Equal(0, revisionsCount);

                    revisionsCount = session.Advanced.Revisions.GetFor<Company>(company2.Id).Count;
                    Assert.Equal(0, revisionsCount);
                }

                using (var session = store.OpenSession())
                {
                    // Force revision with no changes on document
                    session.Advanced.Revisions.ForceRevisionCreationFor(company1Id);

                    // Force revision with changes on document
                    session.Advanced.Revisions.ForceRevisionCreationFor(company2Id);
                    var company2 = session.Load<Company>(company2Id);
                    company2.Name = "HR2 New Name";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    List<Company> revisions = session.Advanced.Revisions.GetFor<Company>(company1Id);
                    var revisionsCount = revisions.Count;
                    Assert.Equal(1, revisionsCount);
                    Assert.Equal("HR1", revisions[0].Name);

                    revisions = session.Advanced.Revisions.GetFor<Company>(company2Id);
                    revisionsCount = revisions.Count;
                    Assert.Equal(1, revisionsCount);
                    Assert.Equal("HR2", revisions[0].Name);

                    // Assert that HasRevisions flag was created on both documents
                    var company = session.Load<Company>(company1Id);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata.TryGetValue("@flags", out var flagsContent);
                    Assert.Equal("HasRevisions", flagsContent);

                    company = session.Load<Company>(company2Id);
                    metadata = session.Advanced.GetMetadataFor(company);
                    metadata.TryGetValue("@flags", out flagsContent);
                    Assert.Equal("HasRevisions", flagsContent);
                }
            }
        }
    }
}
