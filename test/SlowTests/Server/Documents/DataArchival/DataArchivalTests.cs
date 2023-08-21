//-----------------------------------------------------------------------
// <copyright file="ExpirationTests.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents.DataArchival;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Server.Documents.DataArchival
{
    public class DataArchivalTests : RavenTestBase
    {
        public DataArchivalTests(ITestOutputHelper output) : base(output)
        {
        }

        private async Task SetupDataArchival(DocumentStore store)
        {
            var config = new DataArchivalConfiguration {Disabled = false, ArchiveFrequencyInSec = 100};

            await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);
        }

        [Fact]
        public async Task CanSetupDataArchival()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var archiveDateTime = SystemTime.UtcNow.AddMinutes(5);
                    var company = new Company {Name = "Company Name"};
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = archiveDateTime.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }

                var database = await GetDatabase(store.Database);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    
                    DatabaseTopology topology;
                    string nodeTag;
                    
                    using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                    using (serverContext.OpenReadTransaction())
                    {
                        topology = database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, database.Name);
                        nodeTag = database.ServerStore.NodeTag;
                    }
                    
                    var options = new DataArchivalStorage.ArchivedDocumentsOptions(context, SystemTime.UtcNow.AddMinutes(10), topology, nodeTag, 10);

                    var toArchive = database.DocumentsStorage.DataArchivalStorage.GetDocumentsToArchive(options, out _, CancellationToken.None);
                    Assert.Equal(1, toArchive.Count);
                }
            }
        }
        
        [Fact]
        public async Task WillArchiveAllDocumentsToBeArchivedInSingleRun_EvenWhenMoreThanBatchSize()
        {
            DataArchivist.BatchSize = 32;
            const int count = 3200;

            using (var store = GetDocumentStore())
            {
                await SetupDataArchival(store);

                var expiry = SystemTime.UtcNow.AddMinutes(5);
                var metadata = new Dictionary<string, object>
                {
                    [Constants.Documents.Metadata.ArchiveAt] = expiry.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)
                };
                var metadata2 = new Dictionary<string, object>
                {
                    [Constants.Documents.Metadata.ArchiveAt] = expiry.AddMinutes(1).ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)
                };

                for (var i = 0; i < count; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {

                        Company company = new() {Name = "Company Name"};
                        Company company1 = new() {Name = "Company Name"};
                        await session.StoreAsync(company);
                        var metadataFromDoc = session.Advanced.GetMetadataFor(company);
                        metadataFromDoc[Constants.Documents.Metadata.ArchiveAt] = metadata[Constants.Documents.Metadata.ArchiveAt];

                        await session.StoreAsync(company1);
                        var metadataFromDoc2 = session.Advanced.GetMetadataFor(company1);
                        metadataFromDoc2[Constants.Documents.Metadata.ArchiveAt] = metadata2[Constants.Documents.Metadata.ArchiveAt];
                        await session.SaveChangesAsync();
                    }
                }
                
                using (var session = store.OpenAsyncSession())
                {
                    var companies = await session.Query<Company>().Where(x=>x.Name == "Company Name").ToListAsync();
                    Assert.Equal(6400, companies.Count);
                }
                
                 // Activate the archival
                await SetupDataArchival(store);
    
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DataArchivist;
                await documentsArchiver.ArchiveDocs();
                
                using (var session = store.OpenAsyncSession())
                {
                    var companies = await session.Query<Company>().Where(x=>x.Name == "Company Name").ToListAsync();
                    Assert.Equal(0, companies.Count);
                }
            }       
        }

        

        [Fact]
        public async Task ShouldImportTask()
        {
            using (var srcStore = GetDocumentStore())
            using (var dstStore = GetDocumentStore())
            {
                await SetupDataArchival(srcStore);

                var exportFile = GetTempFileName();

                var exportOperation = await srcStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                var operation = await dstStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                var destinationRecord = await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstStore.Database));
                Assert.False(destinationRecord.DataArchival.Disabled);
            }
        }


        [Fact]
        public async Task ThrowsIfUsingWrongArchiveAtDateTimeFormat()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company {Name = "Company Name"};
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = "tomorrow";

                    var error = await Assert.ThrowsAsync<RavenException>(async () => await session.SaveChangesAsync());
                    Assert.Contains($"The archive date format for document '{company.Id.ToLowerInvariant()}' is not valid", error.Message);
                }
            }
        }
        
        
    }
}
