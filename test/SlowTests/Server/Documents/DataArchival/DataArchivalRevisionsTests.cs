using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Util;
using Raven.Server.Documents;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.DataArchival;

public class DataArchivalRevisionsTests: RavenTestBase
{
    public DataArchivalRevisionsTests(ITestOutputHelper output) : base(output)
    {
    }
    
    private async Task SetupDataArchival(IDocumentStore store)
    {
        var config = new DataArchivalConfiguration {Disabled = false, ArchiveFrequencyInSec = 100};

        await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);
    }

    [Fact]
    public async Task WillNotCreateRevisionUponDocumentArchival()
    {
        using (var store = GetDocumentStore())
        {
            // configure the revisions
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration {Disabled = false},
                Collections = new Dictionary<string, RevisionsCollectionConfiguration> {["Companies"] = new() {Disabled = false, MinimumRevisionsToKeep = int.MaxValue}}
            };
            
            await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);
            
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
            
            using (var session = store.OpenAsyncSession())
            {
                company = await session.LoadAsync<Company>(company.Id);
                company.Name += " 2.0";
                await session.SaveChangesAsync();
            }
            
            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var archivedCompanyMetadata = session.Advanced.GetMetadataFor(archivedCompany);

                // check if valid revisions are created
                var revisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(company.Id);
                
                Assert.Equal(2, revisions.Count);
                
                Assert.True(revisionsMetadata[0].TryGetValue(Constants.Documents.Metadata.ArchiveAt, out var _));
                Assert.True(revisionsMetadata[1].TryGetValue(Constants.Documents.Metadata.ArchiveAt, out var _));
                Assert.False(archivedCompanyMetadata.TryGetValue(Constants.Documents.Metadata.ArchiveAt, out var _));
                
                Assert.False(revisionsMetadata[0].TryGetValue(Constants.Documents.Metadata.Archived, out var _));
                Assert.False(revisionsMetadata[1].TryGetValue(Constants.Documents.Metadata.Archived, out var _));
                Assert.Equal(true, archivedCompanyMetadata[Constants.Documents.Metadata.Archived]);
 
                Assert.Equal("Company Name", revisions[1].Name);
                Assert.Equal("Company Name 2.0", revisions[0].Name);
                Assert.Equal("Company Name 2.0", archivedCompany.Name);
            }
        }
    }
}
