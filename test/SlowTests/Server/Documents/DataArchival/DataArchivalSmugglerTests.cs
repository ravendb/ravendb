﻿using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.DataArchival;

public class DataArchivalSmugglerTests: RavenTestBase
{
    public DataArchivalSmugglerTests(ITestOutputHelper output) : base(output)
    {
    }
    
    
    private async Task SetupDataArchival(IDocumentStore store)
    {
        var config = new DataArchivalConfiguration {Disabled = false, ArchiveFrequencyInSec = 100};

        await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);
    }

    [Fact]
    public async void CanFilterOutArchivedDataFromExportAndImport()
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
            
            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();
            

            var toFileWithArchived = Path.Combine(NewDataPath(), "export_with_archived.ravendbdump");
            var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { IncludeArchived = true }, toFileWithArchived);
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            
            var toFileWithoutArchived= Path.Combine(NewDataPath(), "export_with_archived.ravendbdump");
            operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { IncludeArchived = false}, toFileWithoutArchived);
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                        
            
            var importOptionsWithArchived = new DatabaseSmugglerImportOptions { IncludeArchived = true };
            var importOptionsWithoutArchived = new DatabaseSmugglerImportOptions { IncludeArchived = false };
            
            
            // export - include, import - include
            using (var innerStore = GetDocumentStore())
            {
                operation = await innerStore.Smuggler.ImportAsync(importOptionsWithArchived, toFileWithArchived);
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var session = innerStore.OpenSession())
                {
                    Assert.NotNull(session.Load<Company>(company.Id));
                }
            }
            
            
            // export - don't include, import - include
            using (var innerStore = GetDocumentStore())
            {
                operation = await innerStore.Smuggler.ImportAsync(importOptionsWithArchived, toFileWithoutArchived);
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var session = innerStore.OpenSession())
                {
                    Assert.Null(session.Load<Company>(company.Id));
                }
            }
            
            
            // export - include, import - don't include
            using (var innerStore = GetDocumentStore())
            {
                operation = await innerStore.Smuggler.ImportAsync(importOptionsWithoutArchived, toFileWithArchived);
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var session = innerStore.OpenSession())
                {
                    Assert.Null(session.Load<Company>(company.Id));
                }
            }
        }
            
    }

}