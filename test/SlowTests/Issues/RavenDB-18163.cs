using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18163 : RavenTestBase
    {
        public RavenDB_18163(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CreatingARevisionManuallyAfterEnablingRevisionsForAnyCollection()
        {
            using (var store = GetDocumentStore())
            {
                // Define revision settings on Orders collection
                var configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        {
                            "Orders", new RevisionsCollectionConfiguration
                            {
                                PurgeOnDelete = true,
                                MinimumRevisionsToKeep = 5
                            }
                        }
                    }
                };
                var result = await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

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
                    var company = session.Load<Company>(companyId);

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);

                    // Force revisions on a Company document
                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(1, revisionsCount);
                }

                using (var session = store.OpenSession())
                {
                    // Assert that HasRevisions flag was created on both documents
                    var company = session.Load<Company>(companyId);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata.TryGetValue("@flags", out var flagsContent);
                    Assert.Equal("HasRevisions", flagsContent);
                }
            }
        }

        [Fact]
        public async Task CreatingARevisionManuallyAfterEnablingRevisionsForAnyCollectionSameSession()
        {
            using (var store = GetDocumentStore())
            {
                // Define revision settings on Orders collection
                var configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        {
                            "Orders", new RevisionsCollectionConfiguration
                            {
                                PurgeOnDelete = true,
                                MinimumRevisionsToKeep = 5
                            }
                        }
                    }
                };
                var result = await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                string companyId;
                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "HR"
                    };
                    session.Store(company);
                    companyId = company.Id;
                    session.SaveChanges();

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);
                }
                
                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(companyId);
                    company.Name = "HR V2";

                    session.Advanced.Revisions.ForceRevisionCreationFor(company);
                    session.SaveChanges();

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(1, revisionsCount);
                }

                using (var session = store.OpenSession())
                {
                    // Assert that HasRevisions flag was created on both documents
                    var company = session.Load<Company>(companyId);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata.TryGetValue("@flags", out var flagsContent);
                    Assert.Equal("HasRevisions", flagsContent);
                }
            }
        }

        [Fact]
        public async Task CreatingARevisionManuallyAfterEnablingRevisionsForAnyCollectionWhenRevisionIsBlocked()
        {
            using (var store = GetDocumentStore())
            {
                // Define revision settings on Orders collection
                var configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        {"Orders", new RevisionsCollectionConfiguration {PurgeOnDelete = true, MinimumRevisionsToKeep = 5}}
                    }
                };

                var result = await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration)); ;

                using (var session = store.OpenSession())
                {
                    var company = new Company {Name = "HR"};
                    session.Store(company);
                    session.SaveChanges();

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);

                    // Force revisions on a Company document
                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(1, revisionsCount);

                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(1, revisionsCount);

                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata.TryGetValue("@flags", out var flagsContent);
                    Assert.Equal("HasRevisions", flagsContent);
                }
            }
        }
    }

}
