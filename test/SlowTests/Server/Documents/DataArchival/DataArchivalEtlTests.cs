using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Util;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.DataArchival;

public class DataArchivalEtlTests : RavenTestBase
{
    public DataArchivalEtlTests(ITestOutputHelper output) : base(output)
    {

    }
    
    
    private async Task SetupDataArchival(DocumentStore store)
    {
        var config = new DataArchivalConfiguration {Disabled = false, ArchiveFrequencyInSec = 100};

        await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);
    }

    [Fact]
    public async Task CanFilterOutUnarchivedDocumentsInEtl()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            Etl.AddEtl(src, new RavenEtlConfiguration
            {
                Name = "simulate",
                ConnectionStringName = "my-con",
                Transforms = new List<Transformation>
                {
                    new()
                    {
                        Collections = {"Companies"},
                        Name = "SkipUnarchived",
                        Script =
                            "var archived = this[\"@metadata\"][\"@archived\"]" + Environment.NewLine +
                            "if (archived !== undefined && archived === true) {" + Environment.NewLine +
                            "    loadToCompanies(this);" + Environment.NewLine +
                            "}"
                    }
                }
            },
            new RavenConnectionString
            {
                Name = "my-con",
                Database = dest.Database,
                TopologyDiscoveryUrls = dest.Urls
            });
            
            var etlDone = Etl.WaitForEtlToComplete(src);
            
            // Insert document with archive time before activating the archival
            var company = new Company {Name = "Company Name", Address1 = "Dabrowskiego 6"};
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }
            
            
            etlDone.Wait(TimeSpan.FromMinutes(1));
                
            // Make sure that the company has been skipped from ETL
            using (var session = dest.OpenAsyncSession())
            {
                var companiesOnDestination = await session.Query<Company>().ToListAsync();
                Assert.Equal(0, companiesOnDestination.Count);
            }

            etlDone.Reset();
            
            // Activate the archival
            await SetupDataArchival(src);

            var database = await Databases.GetDocumentDatabaseInstanceFor(src);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = src.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);
            }
            
            etlDone.Wait(TimeSpan.FromMinutes(1));
            
            // Make sure that the company has been propagated to the destination upon archival 
            using (var session = dest.OpenAsyncSession())
            {
                var companiesOnDestination = await session.Query<Company>().ToListAsync();
                Assert.Equal(1, companiesOnDestination.Count);
            }
        }
    }
    
    [Fact]
    public async Task CanFilterOutArchivedDocumentsInEtl()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            
            // Insert document with archive time before activating the archival
            var company = new Company {Name = "Company Name", Address1 = "Dabrowskiego 6"};
            var company2 = new Company {Name = "OG IT", Address1 = "Julianowo 6A"};
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }
            
            
            // Activate the archival
            await SetupDataArchival(src);

            var database = await Databases.GetDocumentDatabaseInstanceFor(src);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = src.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);
            }
                        
            Etl.AddEtl(src, new RavenEtlConfiguration
            {
                Name = "simulate",
                ConnectionStringName = "my-con",
                Transforms = new List<Transformation>
                {
                    new()
                    {
                        Collections = {"Companies"},
                        Name = "SkipUnarchived",
                        Script =
                            "var archived = this[\"@metadata\"][\"@archived\"]" + Environment.NewLine +
                            "if (archived == undefined) {" + Environment.NewLine +
                            "    loadToCompanies(this);" + Environment.NewLine +
                            "}"
                    }
                }
            },
            new RavenConnectionString
            {
                Name = "my-con",
                Database = dest.Database,
                TopologyDiscoveryUrls = dest.Urls
            });
            
            var etlDone = Etl.WaitForEtlToComplete(src);
            etlDone.Wait(TimeSpan.FromMinutes(1));
                
            // Make sure that the company has been skipped from ETL
            using (var session = dest.OpenAsyncSession())
            {
                var companiesOnDestination = await session.Query<Company>().ToListAsync();
                Assert.Equal(0, companiesOnDestination.Count);
            }
            
            etlDone.Reset();
                        
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(company2);
                await session.SaveChangesAsync();
            }

            
            etlDone.Wait(TimeSpan.FromMinutes(1));
            
            // Make sure that the company has been propagated to the destination upon archival 
            using (var session = dest.OpenAsyncSession())
            {
                var companiesOnDestination = await session.Query<Company>().ToListAsync();
                Assert.Equal(1, companiesOnDestination.Count);
            }
        }
    }
}
