using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Queries;
using Raven.Client.Util;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.DataArchival;

public class DataArchivalPatchingTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private async Task SetupDataArchival(IDocumentStore store)
    {
        var config = new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 100 };

        await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);
    }

    [RavenFact(RavenTestCategory.Patching)]
    public async Task CanScheduleDocumentsToBeArchivedUsingPatch()
    {
        using (var store = GetDocumentStore())
        {
            // Insert document 
            var company = new Company { Name = "Company Name" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                await session.SaveChangesAsync();
            }
            
            // Use patch to schedule archival
            var operation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery()
            {
                Query = "from Companies update { archived.archiveAt(this, \"" + retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite) + "\") }"
            }));
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

            // Make sure that the company is not skipped while indexing yet
            using (var session = store.OpenAsyncSession())
            {
                List<Company> companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                Assert.Equal(1, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                // Make sure that the company is skipped while indexing
                var companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                Assert.Equal(0, companies.Count);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Patching)]
    public async Task CanRevertDataArchivalUsingPatch()
    {
        using (var store = GetDocumentStore())
        {
            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is not skipped while indexing yet
            using (var session = store.OpenAsyncSession())
            {
                List<Company> companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                Assert.Equal(1, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                // Make sure that the company is skipped while indexing (auto map index)
                var companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                Assert.Equal(0, companies.Count);
            }

            // Unarchive document using patch
            var operation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery()
            {
                Query = "from Companies update { archived.unarchive(this) }"
            }));
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                // Make sure that item is unarchived, and the flags & markers are gone 
                var unarchivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(unarchivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.DoesNotContain(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.DoesNotContain(Constants.Documents.Metadata.Flags, metadata.Keys); // the last flag in the @flags was 'Archived', so now the property should completely disappear

                // Make sure that the company is not anymore skipped while indexing 

                var companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                Assert.Equal(1, companies.Count);
            }
        }
    }

    [RavenFact(RavenTestCategory.Patching)]
    public async Task PuttingDocumentWithArchivedFlagInMetadataUsingPatchWillDropTheFlag()
    {
        using (var store = GetDocumentStore())
        {
            // Insert document with archive time before activating the archival
            var company = new Company {Name = "Company Name"};
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }
            
            // Make sure that the company is not skipped while indexing yet
            using (var session = store.OpenAsyncSession())
            {
                List<Company> companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                Assert.Equal(1, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);
            
            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                // Make sure that the company is skipped while indexing (auto map index)
                var companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                Assert.Equal(0, companies.Count);
            }
            
            var operation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery()
            {
                Query = "from Companies update { put(\"companies/\", this); }"
            }));
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

            using (var session = store.OpenAsyncSession())
            {
                var originalCompany = await session.LoadAsync<Company>(company.Id);
                Assert.Equal("Company Name", originalCompany.Name);
                var metadata = session.Advanced.GetMetadataFor(originalCompany);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                
                // Make sure that the company is skipped while indexing (auto map index)
                var companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                Assert.Equal(1, companies.Count);
                Assert.Equal("Company Name", companies[0].Name);
   
                metadata = session.Advanced.GetMetadataFor(companies[0]);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.DoesNotContain(Constants.Documents.Metadata.Archived, metadata.Keys);
            }
        }
    }
    
    
    [RavenFact(RavenTestCategory.Patching)]
    public async Task SchedulingAlreadyArchivedDocumentsToBeArchivedUsingPatchWontAddArchiveAtFieldToDocMetadata()
    {
        using (var store = GetDocumentStore())
        {
            // Insert document 
            var company = new Company { Name = "Company Name" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            var retiresPatch = SystemTime.UtcNow.AddMinutes(15);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }
            
            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            
            // Use patch to schedule archival
            var operation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery()
            {
                Query = "from Companies update { archived.archiveAt(this, \"" + retiresPatch.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite) + "\") }"
            }));
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                // Make sure that the company is skipped while indexing
                var companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                Assert.Equal(0, companies.Count);
            }
        }
    }
}
