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
                Default = new RevisionsCollectionConfiguration {Disabled = false, MinimumRevisionsToKeep = 10},
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
            
            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);
                
                
                // check if revision isn't created
                var revisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                Assert.Equal(1,revisions.Count);
            }
        }
    }
}
