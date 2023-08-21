using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Util;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.DataArchival;

public class DataArchivalReplicationTests : ReplicationTestBase
{

    public DataArchivalReplicationTests(ITestOutputHelper output) : base(output)
    {
    }

    private async Task SetupDataArchival(DocumentStore store)
    {
        var config = new DataArchivalConfiguration {Disabled = false, ArchiveFrequencyInSec = 100};

        await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);
    }

    [Fact]
    public async Task CanIndexOnlyUnarchivedDocuments_AutoMapIndex_WithReplication()
    {
        using (var store1 = GetDocumentStore())
        using (var store2 = GetDocumentStore())
        {
            await SetupReplicationAsync(store1, store2);
            await SetupReplicationAsync(store2, store1);

            var company = new Company {Name = "Company Name"};
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is not skipped while indexing yet
            using (var session = store1.OpenAsyncSession())
            {
                List<Company> companies = await session.Query<Company>().Where(x => x.Name == "Company Name", true).ToListAsync();
                Assert.Equal(1, companies.Count);
            }
            
            // Wait for document to replicate
            await WaitForDocumentToReplicateAsync<Company>(store2, company.Id, 15 * 1000);
            
            using (var session = store2.OpenAsyncSession())
            {
                List<Company> companies = await session.Query<Company>().Where(x => x.Name == "Company Name", true).ToListAsync();
                Assert.Equal(1, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store1);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store1);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = store1.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);
            }
            
            // Wait for document to replicate
            await WaitForDocumentToReplicateAsync<Company>(store2, company.Id, 15 * 1000);
            
            
            bool archived = false;
            int failures = 0;
                
            // Wait for data archival
            while (!archived)
            {
                using (var session = store2.OpenAsyncSession())
                {
                    var loadedCompany = await session.LoadAsync<Company>(company.Id);
                    archived = session.Advanced.GetMetadataFor(loadedCompany).TryGetValue(Constants.Documents.Metadata.Archived, out object _);
                    Thread.Sleep(100);
                    if (++failures > 100)
                        break;
                }
            }
            
            using (var session = store2.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);
                List<Company> companies = await session.Query<Company>().Where(x => x.Name == "Company Name", true).ToListAsync();
                Assert.Equal(0, companies.Count);
            }
        }
    }
}


        
