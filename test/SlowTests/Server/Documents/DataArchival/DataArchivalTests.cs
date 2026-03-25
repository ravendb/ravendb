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
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.DocumentsCompression;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.DataArchival;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;


namespace SlowTests.Server.Documents.DataArchival
{
    public class DataArchivalTests : RavenTestBase
    {
        public DataArchivalTests(ITestOutputHelper output) : base(output)
        {
        }

        private async Task SetupDataArchival(DocumentStore store)
        {
            var config = new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 100 };

            await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);
        }

        [RavenTheory(RavenTestCategory.ExpirationRefresh | RavenTestCategory.Compression)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CanSetupDataArchival(bool compressed)
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                        
                    }
                }
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    var archiveDateTime = SystemTime.UtcNow.AddMinutes(5);
                    var company = new Company { Name = "Company Name" };
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

                    var options = new BackgroundWorkParameters(context, SystemTime.UtcNow.AddMinutes(10), topology, nodeTag, 10);
                    var totalCount = 0;

                    var toArchive = database.DocumentsStorage.DataArchivalStorage.GetDocuments(options, ref totalCount, out _, CancellationToken.None);
                    Assert.Equal(1, toArchive.Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ExpirationRefresh | RavenTestCategory.Compression)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task WillArchiveAllDocumentsToBeArchivedInSingleRun_EvenWhenMoreThanBatchSize(bool compressed)
        {
            DataArchivist.BatchSize = 32;
            const int count = 3200;

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                    }
                }
            }))
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

                        Company company = new() { Name = "Company Name" };
                        Company company1 = new() { Name = "Company Name" };
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
                    var companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                    Assert.Equal(6400, companies.Count);
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
                    WaitForUserToContinueTheTest(store);
                    var companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                    Assert.Equal(0, companies.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ExpirationRefresh | RavenTestCategory.Revisions)]
        public async Task ArchiveFlagShouldNotBeRemovedAfterDocumentUpdate()
        {
            using (var store = GetDocumentStore())
            {
                // Enable revisions
                // ================
                var usersConfig = new RevisionsCollectionConfiguration()
                {
                    MinimumRevisionsToKeep = int.MaxValue,
                    Disabled = false
                };
                
                var revisionsConfig = new RevisionsConfiguration()
                {
                    Default = new RevisionsCollectionConfiguration {Disabled = false},
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>()
                    {
                        { "Users", usersConfig }
                    }
                };
                
                var configureRevisionsOp = new ConfigureRevisionsOperation(revisionsConfig);
                store.Maintenance.Send(configureRevisionsOp);
                
                // Create document 
                // ===============
                var user = new User {Name = "aaa"};
                var archiveAt = SystemTime.UtcNow.AddSeconds(30);
                
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = archiveAt.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    session.SaveChanges();
                }
                
                // Activate the archival
                await SetupDataArchival(store);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DataArchivist;
                await documentsArchiver.ArchiveDocs();

                await Indexes.WaitForIndexingAsync(store);

                
                // Verify that @flags contains the "Archived" flag
                // ===============================================
                using (var session = store.OpenSession())
                {
                    var archivedUser = session.Load<User>(user.Id);
                    var archivedUserMetadata = session.Advanced.GetMetadataFor(archivedUser);
                    
                    var flagsValue = archivedUserMetadata["@flags"];
                    Assert.True(flagsValue.ToString().Contains("Archived"));
                }
                
                // Modify the document AFTER it was archived
                // =========================================
                using (var session = store.OpenSession())
                {
                    user = session.Load<User>(user.Id);
                    user.Name += " some text";
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    // Check revisions:
                    var revisions = session.Advanced.Revisions.GetFor<User>(user.Id);
                    Assert.Equal(2, revisions.Count);
                    
                    // Check the content of @flags in the metadata:
                    var archivedUser = session.Load<User>(user.Id);
                    var archivedUserMetadata = session.Advanced.GetMetadataFor(archivedUser);
                    
                    var flagsValue = archivedUserMetadata["@flags"];
                    Assert.True(flagsValue.ToString().Contains("HasRevisions"));
                    
                    Assert.True(flagsValue.ToString().Contains("Archived"));
                }
            }
        }
        
        [RavenTheory(RavenTestCategory.ExpirationRefresh)]
        [InlineData(false)]
        [InlineData(true)]
        public void ShouldNotApplyArchivedMetadataForNewDocuments(bool archivedValue)      
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), null, "archived/1");
                    var metadata = session.Advanced.GetMetadataFor(session.Load<User>("archived/1"));
                    metadata[Constants.Documents.Metadata.Archived] = archivedValue;
                    session.SaveChanges();
                }

                // Assert no flag in "@flags", assert no "@archived"
                // ===============================================
                using (var session = store.OpenSession())
                {
                    var archivedUser = session.Load<User>("archived/1");
                    var archivedUserMetadata = session.Advanced.GetMetadataFor(archivedUser);
                    Assert.False(archivedUserMetadata.ContainsKey("@archived"));
                    if (archivedUserMetadata.TryGetValue("@flags", out object flagsValue))
                    {
                        Assert.DoesNotContain(flagsValue.ToString(), "Archived");
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.ExpirationRefresh)]
        [InlineData(false)]
        [InlineData(true)]
        public void ShouldNotApplyArchivedMetadataForUnarchivedDocsUpdates(bool archivedValue)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "aaa" }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata[Constants.Documents.Metadata.Archived] = archivedValue;
                    session.SaveChanges();
                }
                // Assert no "@flags", assert no "@archived"
                // ===============================================
                using (var session = store.OpenSession())
                {
                    var archivedUser = session.Load<User>("users/1");
                    var archivedUserMetadata = session.Advanced.GetMetadataFor(archivedUser);
                    Assert.False(archivedUserMetadata.ContainsKey("@archived"));
                    if (archivedUserMetadata.TryGetValue("@flags", out object flagsValue))
                    {
                        Assert.DoesNotContain(flagsValue.ToString(), "Archived");
                    }
                }
            }
        }
        
        
        [RavenFact(RavenTestCategory.ExpirationRefresh)]
        public async Task ShouldRebuildArchivedInMetadataIfArchivedDocumentUpdateRemovesIt ()
        {
            using (var store = GetDocumentStore())
            {
                // Create document 
                // ===============
                var user = new User {Name = "aaa"};
                var archiveAt = SystemTime.UtcNow.AddSeconds(30);
                
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = archiveAt.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    session.SaveChanges();
                }
                
                // Activate the archival
                await SetupDataArchival(store);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DataArchivist;
                await documentsArchiver.ArchiveDocs();

                await Indexes.WaitForIndexingAsync(store);

                
                // Verify that @flags contains the "Archived" flag, try to delete it
                // ===============================================
                using (var session = store.OpenSession())
                {
                    var archivedUser = session.Load<User>(user.Id);
                    var archivedUserMetadata = session.Advanced.GetMetadataFor(archivedUser);
                    
                    var flagsValue = archivedUserMetadata["@flags"];
                    Assert.Contains(flagsValue.ToString(), "Archived");
                    
                    archivedUserMetadata.Remove(Constants.Documents.Metadata.Archived);
                    session.SaveChanges();
                }

                // Assert flag and "@archived" still here
                // ===============================================
                using (var session = store.OpenSession())
                {
                    var archivedUser = session.Load<User>(user.Id);
                    var archivedUserMetadata = session.Advanced.GetMetadataFor(archivedUser);
                    
                    Assert.True(archivedUserMetadata.TryGetValue("@archived", out object archivedValue));
                    Assert.Equal("True", archivedValue.ToString());
                    
                    Assert.True(archivedUserMetadata.TryGetValue("@flags", out object flagsValue));
                    Assert.True(flagsValue.ToString().Contains("Archived"));
                }
            }
        }
        
        [RavenFact(RavenTestCategory.ExpirationRefresh)]
        public async Task ShouldRevertArchivedMetadataToTrueIfArchivedDocumentUpdateSetItToFalse()
        {
            using (var store = GetDocumentStore())
            {
                // Create document 
                // ===============
                var user = new User {Name = "aaa"};
                var archiveAt = SystemTime.UtcNow.AddSeconds(30);
                
                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = archiveAt.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    session.SaveChanges();
                }
                
                // Activate the archival
                await SetupDataArchival(store);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DataArchivist;
                await documentsArchiver.ArchiveDocs();

                await Indexes.WaitForIndexingAsync(store);

                
                // Verify that @flags contains the "Archived" flag, try to set it to false
                // ===============================================
                using (var session = store.OpenSession())
                {
                    var archivedUser = session.Load<User>(user.Id);
                    var archivedUserMetadata = session.Advanced.GetMetadataFor(archivedUser);
                    
                    var flagsValue = archivedUserMetadata["@flags"];
                    Assert.True(flagsValue.ToString().Contains("Archived"));
                    
                    archivedUserMetadata[Constants.Documents.Metadata.Archived] = false;
                    session.SaveChanges();
                }

                // Assert flag and "@archived: true" still here
                // ===============================================
                using (var session = store.OpenSession())
                {
                    var archivedUser = session.Load<User>(user.Id);
                    var archivedUserMetadata = session.Advanced.GetMetadataFor(archivedUser);
                    
                    Assert.True(archivedUserMetadata.TryGetValue("@flags", out object flagsValue));
                    Assert.Contains(flagsValue.ToString(), "Archived");
                    
                    Assert.True(archivedUserMetadata.TryGetValue("@archived", out object archivedValue));
                    Assert.Equal("True", archivedValue.ToString());
                }
            }
        }

        [RavenTheory(RavenTestCategory.ExpirationRefresh | RavenTestCategory.Compression)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ShouldImportTask(bool compressed)
        {
            using (var srcStore = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                    }
                }
            }))
            using (var dstStore = GetDocumentStore(new Options 
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                    }
                }
            }))
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

        [RavenTheory(RavenTestCategory.ExpirationRefresh | RavenTestCategory.Compression)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ThrowsIfUsingWrongArchiveAtDateTimeFormat(bool compressed)
        {
            using (var store = GetDocumentStore(new Options 
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                    }
                }
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company { Name = "Company Name" };
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = "tomorrow";

                    var error = await Assert.ThrowsAsync<RavenException>(async () => await session.SaveChangesAsync());
                    Assert.Contains($"The due date format for document '{company.Id.ToLowerInvariant()}' is not valid", error.Message);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Configuration | RavenTestCategory.Compression)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task ArchiveDocsWithMaxItemsToProcessConfiguredShouldWork(Options options, bool compressed)
        {
            using (var store = GetDocumentStore(options))
            {
                if (compressed)
                {
                    var documentsCompression = new DocumentsCompressionConfiguration(true, true);
                    store.Maintenance.Send(new UpdateDocumentsCompressionConfigurationOperation(documentsCompression));
                }
                // Insert documents with ArchiveAt before activating the archival
                var archiveDateTime = SystemTime.UtcNow.AddMinutes(5);
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var company = new Company { Name = "Company Name", Id = $"companies/{i}$companies/1" };
                        await session.StoreAsync(company);
                        var metadata = session.Advanced.GetMetadataFor(company);
                        metadata[Constants.Documents.Metadata.ArchiveAt] = archiveDateTime.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                        await session.SaveChangesAsync();
                    }
                }

                var config = new DataArchivalConfiguration
                {
                    Disabled = false,
                    ArchiveFrequencyInSec = (long)TimeSpan.FromMinutes(10).TotalSeconds,
                    MaxItemsToProcess = 9
                };

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "companies/1");
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config, database.Name);

                DatabaseTopology topology;

                string nodeTag;

                using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                using (serverContext.OpenReadTransaction())
                {
                    topology = database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, database.Name);
                    nodeTag = database.ServerStore.NodeTag;
                }

                DateTime time = SystemTime.UtcNow.AddMinutes(10);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var archiveOptions = new BackgroundWorkParameters(context, time, topology, nodeTag, AmountToTake: 10, MaxItemsToProcess: 10);
                    var totalCount = 0;
                    var toArchive = database.DocumentsStorage.DataArchivalStorage.GetDocuments(archiveOptions, ref totalCount, out _, CancellationToken.None);
                    Assert.Equal(10, totalCount);
                }

                var dataArchivist = database.DataArchivist;
                await dataArchivist.ArchiveDocs();

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var archiveOptions = new BackgroundWorkParameters(context, time, topology, nodeTag, AmountToTake: 10, MaxItemsToProcess: 10);
                    var totalCount = 0;
                    var toArchive = database.DocumentsStorage.DataArchivalStorage.GetDocuments(archiveOptions, ref totalCount, out _, CancellationToken.None);
                    Assert.Equal(1, totalCount);
                }
            }
        }
    }
}