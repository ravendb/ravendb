using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Documents.Revisions;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.Revisions
{
    public class RevertRevisionsTests : ReplicationTestBase
    {
        [Fact]
        public async Task Revert()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                var db = await GetDocumentDatabaseInstanceFor(store);
                var result = (RevisionsStorage.RevertResult) await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60));

                Assert.Equal(2, result.Progress.ScannedRevisions);
                Assert.Equal(1, result.Progress.ScannedDocuments);
                Assert.Equal(1, result.Progress.RevertedDocuments);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(3, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                    Assert.Equal("Company Name", companiesRevisions[2].Name);
                }
            }
        }

        [Fact]
        public async Task DontRevertOldDocument()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = "Hibernating Rhinos";
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                DateTime last = DateTime.UtcNow;

                var db = await GetDocumentDatabaseInstanceFor(store);
                var result = (RevisionsStorage.RevertResult) await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60));

                Assert.Equal(2, result.Progress.ScannedRevisions);
                Assert.Equal(1, result.Progress.ScannedDocuments);
                Assert.Equal(0, result.Progress.RevertedDocuments);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(2, companiesRevisions.Count);

                    Assert.Equal("Hibernating Rhinos", companiesRevisions[0].Name);
                    Assert.Equal("Company Name", companiesRevisions[1].Name);
                }
            }
        }

        [Fact]
        public async Task DontRevertNewDocument()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = DateTime.UtcNow;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                var db = await GetDocumentDatabaseInstanceFor(store);
                var result = (RevisionsStorage.RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last.Add(TimeSpan.FromMinutes(1)), TimeSpan.FromMinutes(60));

                Assert.Equal(1, result.Progress.ScannedRevisions);
                Assert.Equal(1, result.Progress.ScannedDocuments);
                Assert.Equal(0, result.Progress.RevertedDocuments);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(1, companiesRevisions.Count);
                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                }
            }
        }

        [Fact]
        public async Task RevertFromDeleted()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = default;

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                    last = DateTime.UtcNow;
                    session.Delete(company.Id);
                    await session.SaveChangesAsync();
                }

                var db = await GetDocumentDatabaseInstanceFor(store);
                var result = (RevisionsStorage.RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60));

                Assert.Equal(2, result.Progress.ScannedRevisions);
                Assert.Equal(1, result.Progress.ScannedDocuments);
                Assert.Equal(1, result.Progress.RevertedDocuments);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(3, companiesRevisions.Count);

                    Assert.Equal("Company Name", companiesRevisions[0].Name);
                    Assert.Equal(null, companiesRevisions[1].Name);
                    Assert.Equal("Company Name", companiesRevisions[2].Name);
                }
            }
        }

        [Fact]
        public async Task RevertToDeleted()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();

                    session.Delete(company);
                    await session.SaveChangesAsync();

                    last = DateTime.UtcNow;

                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                var db = await GetDocumentDatabaseInstanceFor(store);
                var result = (RevisionsStorage.RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60));

                Assert.Equal(3, result.Progress.ScannedRevisions);
                Assert.Equal(1, result.Progress.ScannedDocuments);
                Assert.Equal(1, result.Progress.RevertedDocuments);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(4, companiesRevisions.Count);

                    Assert.Equal(null, companiesRevisions[0].Name);
                    Assert.Equal("Company Name", companiesRevisions[1].Name);
                    Assert.Equal(null, companiesRevisions[2].Name);
                    Assert.Equal("Company Name", companiesRevisions[3].Name);
                }
            }
        }

        [Fact]
        public async Task DontRevertToConflicted()
        {
            // put at 8:30
            // conflicted at 8:50
            // resolved at 9:10
            // will revert to 9:00

            using (var store1 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore())
            {
                DateTime last = default;
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), "keep-conflicted-revision-insert-order");
                    await session.SaveChangesAsync();

                    var company = new Company
                    {
                        Name = "Name2"
                    };
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var company = new Company
                    {
                        Name = "Name1"
                    };
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("foo/bar");
                    Assert.Equal(1, companiesRevisions.Count);
                }

                await SetupReplicationAsync(store2, store1);
                WaitUntilHasConflict(store1, "foo/bar");

                last = DateTime.UtcNow;

                using (var session = store1.OpenAsyncSession())
                {
                    var company = new Company
                    {
                        Name = "Resolver"
                    };
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                }

                var db = await GetDocumentDatabaseInstanceFor(store1);
                var result = (RevisionsStorage.RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last.Add(TimeSpan.FromMinutes(1)), TimeSpan.FromMinutes(60));

                Assert.Equal(3, result.Progress.ScannedRevisions);
                Assert.Equal(1, result.Progress.ScannedDocuments);
                Assert.Equal(0, result.Progress.RevertedDocuments);

                using (var session = store1.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("foo/bar");
                    Assert.Equal(3, companiesRevisions.Count);
                }
            }
        }

        [Fact]
        public async Task RevertResolvedConflictByRemoteToOriginal()
        {
            // put was at 8:50
            // conflict at 9:10
            // resolved at 9:15
            // will revert to 9:00

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                DateTime last = default;

                using (var session = store1.OpenAsyncSession())
                {
                    var company = new Company
                    {
                        Name = "Name1"
                    };
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                    last = session.Advanced.GetLastModifiedFor(company).Value;
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("foo/bar");
                    Assert.Equal(1, companiesRevisions.Count);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var company = new Company
                    {
                        Name = "Name2"
                    };
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();

                   
                    await session.StoreAsync(new Company(), "marker");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store2, store1);
                WaitForDocument(store1, "marker");

                var db = await GetDocumentDatabaseInstanceFor(store1);
                var result = (RevisionsStorage.RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60));

                Assert.Equal(3, result.Progress.ScannedRevisions);
                Assert.Equal(1, result.Progress.ScannedDocuments);
                Assert.Equal(1, result.Progress.RevertedDocuments);

                using (var session = store1.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("foo/bar");
                    Assert.Equal(4, companiesRevisions.Count);

                    Assert.Equal("Name1", companiesRevisions[0].Name);
                    var metadata = session.Advanced.GetMetadataFor(companiesRevisions[0]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Reverted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name2", companiesRevisions[1].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[1]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.FromReplication | DocumentFlags.Resolved).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name1", companiesRevisions[2].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[2]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Conflicted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name2", companiesRevisions[3].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[2]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Conflicted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                }
            }
        }

        [Fact]
        public async Task RevertResolvedConflictByLocalToOriginal()
        {
            // put was at 8:50
            // conflict at 9:10
            // resolved at 9:15
            // will revert to 9:00

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                DateTime last = default;

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), "keep-conflicted-revision-insert-order");
                    await session.SaveChangesAsync();

                    var company = new Company
                    {
                        Name = "Name2"
                    };
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var company = new Company
                    {
                        Name = "Name1"
                    };
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                    last = session.Advanced.GetLastModifiedFor(company).Value;
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("foo/bar");
                    Assert.Equal(1, companiesRevisions.Count);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), "marker");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store2, store1);
                WaitForDocument(store1, "marker");

                var db = await GetDocumentDatabaseInstanceFor(store1);
                var result = (RevisionsStorage.RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60));

                Assert.Equal(3, result.Progress.ScannedRevisions);
                Assert.Equal(1, result.Progress.ScannedDocuments);
                Assert.Equal(1, result.Progress.RevertedDocuments);

                using (var session = store1.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("foo/bar");
                    Assert.Equal(4, companiesRevisions.Count);

                    Assert.Equal("Name1", companiesRevisions[0].Name);
                    var metadata = session.Advanced.GetMetadataFor(companiesRevisions[0]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Reverted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name1", companiesRevisions[1].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[1]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Resolved).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name1", companiesRevisions[2].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[2]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Conflicted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name2", companiesRevisions[3].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[3]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                }
            }
        }

        [Fact]
        public async Task RevertResolvedConflictByRemoteToDeleted()
        {
            // deleted was at 8:50
            // conflict at 9:10
            // resolved at 9:15
            // will revert to 9:00

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                DateTime last = default;

                using (var session = store1.OpenAsyncSession())
                {
                    var company = new Company
                    {
                        Name = "Name1"
                    };
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();

                    session.Delete("foo/bar");
                    await session.SaveChangesAsync();

                    last = DateTime.UtcNow;
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var company = new Company
                    {
                        Name = "Name2"
                    };
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();


                    await session.StoreAsync(new Company(), "marker");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store2, store1);
                WaitForDocument(store1, "marker");

                var db = await GetDocumentDatabaseInstanceFor(store1);
                var result = (RevisionsStorage.RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60));

                Assert.Equal(4, result.Progress.ScannedRevisions);
                Assert.Equal(1, result.Progress.ScannedDocuments);
                Assert.Equal(1, result.Progress.RevertedDocuments);

                using (var session = store1.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("foo/bar");
                    Assert.Equal(5, companiesRevisions.Count);

                    Assert.Equal(null, companiesRevisions[0].Name);
                    var metadata = session.Advanced.GetMetadataFor(companiesRevisions[0]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.DeleteRevision | DocumentFlags.Reverted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name2", companiesRevisions[1].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[1]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.FromReplication | DocumentFlags.Resolved).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal(null, companiesRevisions[2].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[2]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.DeleteRevision | DocumentFlags.Conflicted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name2", companiesRevisions[3].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[3]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name1", companiesRevisions[4].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[4]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                }
            }
        }

        [Fact]
        public async Task RevertResolvedConflictByLocalToDeleted()
        {
            // deleted was at 8:50
            // conflict at 9:10
            // resolved at 9:15
            // will revert to 9:00

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                DateTime last = default;

                using (var session = store2.OpenAsyncSession())
                {
                    var company = new Company
                    {
                        Name = "Name2"
                    };
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();


                    await session.StoreAsync(new Company(), "marker");
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var company = new Company
                    {
                        Name = "Name1"
                    };
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();

                    session.Delete("foo/bar");
                    await session.SaveChangesAsync();

                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("foo/bar");
                    Assert.Equal(2, companiesRevisions.Count);

                    last = DateTime.UtcNow;
                }
               

                await SetupReplicationAsync(store2, store1);
                WaitForDocument(store1, "marker");

                using (var session = store1.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("foo/bar");
                    Assert.Equal(4, companiesRevisions.Count);
                }

                var db = await GetDocumentDatabaseInstanceFor(store1);
                var result = (RevisionsStorage.RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60));

                Assert.Equal(4, result.Progress.ScannedRevisions);
                Assert.Equal(1, result.Progress.ScannedDocuments);
                Assert.Equal(1, result.Progress.RevertedDocuments);

                using (var session = store1.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("foo/bar");
                    Assert.Equal(5, companiesRevisions.Count);

                    Assert.Equal(null, companiesRevisions[0].Name);
                    var metadata = session.Advanced.GetMetadataFor(companiesRevisions[0]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.DeleteRevision | DocumentFlags.Reverted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal(null, companiesRevisions[1].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[1]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.DeleteRevision | DocumentFlags.Resolved).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name2", companiesRevisions[3].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[3]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                    
                    Assert.Equal(null, companiesRevisions[2].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[2]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.DeleteRevision | DocumentFlags.Conflicted).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                    Assert.Equal("Name1", companiesRevisions[4].Name);
                    metadata = session.Advanced.GetMetadataFor(companiesRevisions[4]);
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision).ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                }
            }
        }
    }
}
