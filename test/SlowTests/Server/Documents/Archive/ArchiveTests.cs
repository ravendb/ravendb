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
using Raven.Client.Documents.Operations.Archival;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Archival;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Server.Documents.Archive
{
    public class ArchiveTests : RavenTestBase
    {
        public ArchiveTests(ITestOutputHelper output) : base(output)
        {
        }

        private async Task SetupArchival(DocumentStore store)
        {
            var config = new ArchivalConfiguration
            {
                Disabled = false,
                ArchiveFrequencyInSec = 100,
            };

            await ArchivalHelper.SetupArchival(store, Server.ServerStore, config);
        }

        [Fact]
        public async Task CanSetupArchive()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var archiveDateTime = SystemTime.UtcNow.AddMinutes(5);
                    var company = new Company {Name = "Company Name"};
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.Archive] = archiveDateTime.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }

                var database = await GetDatabase(store.Database);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var options = new ArchivalStorage.ArchivedDocumentsOptions(context, SystemTime.UtcNow.AddMinutes(10), 10);

                    var toArchive = database.DocumentsStorage.ArchivalStorage.GetDocumentsToArchive(options, out _, CancellationToken.None);
                    Assert.Equal(1, toArchive.Count);
                }
            }
        }

        [Fact]
        public async Task CanAddEntityWithArchive_BeforeActivatingArchival_WillMigrateToAnotherCollectionAfterArchive()
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
                    metadata[Constants.Documents.Metadata.Archive] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }

                // Activate the archival
                await SetupArchival(store);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DocumentsArchivist;
                await documentsArchiver.ArchiveDocs();

                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>(company.Id);
                    var metadata = session.Advanced.GetMetadataFor(company2);
                    Assert.DoesNotContain(Constants.Documents.Metadata.Archive, metadata.Keys);
                    Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                    Assert.Equal("CompaniesArchived", metadata[Constants.Documents.Metadata.Collection]);
                }
            }
        }
        
        [Fact]
        public async Task ShouldImportTask()
        {
            using (var srcStore = GetDocumentStore())
            using (var dstStore = GetDocumentStore())
            {
                await SetupArchival(srcStore);

                var exportFile = GetTempFileName();

                var exportOperation = await srcStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                var operation = await dstStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
                
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                var destinationRecord = await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstStore.Database));
                Assert.False(destinationRecord.Archival.Disabled); 
            }
        }

        [Fact]
        public async Task CanGetRevisionsForArchivedDocuments()
        {
            var company = new Company {Name = "Company Name"};
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.Archive] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    company3.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }
                
                await SetupArchival(store);
                
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DocumentsArchivist;
                await documentsArchiver.ArchiveDocs();

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(4, companiesRevisions.Count);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[^2].Name);
                    Assert.Equal("Company Name", companiesRevisions[^1].Name);
                }
            }        
        }

        [Fact]
        public async Task CanGetTimeSeriesForArchivedDocuments()
        {
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var user = new User {Name = "Gracjan"};
                    session.Store(user, "users/gracjan");

                    session.TimeSeriesFor("users/gracjan", "Heartrate")
                        .Append(baseline.AddMinutes(1), 59d, "watches/fitbit");
                    
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata[Constants.Documents.Metadata.Archive] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    
                    session.SaveChanges();
                }
                
                await SetupArchival(store);
                
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DocumentsArchivist;
                await documentsArchiver.ArchiveDocs();

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/gracjan", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue);

                    var val = vals.Single();
                    
                    Assert.Equal(new[] { 59d }, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public async Task CanGetCountersForArchivedDocument()
        {
            int docsPerCollection = 2;
            int countersPerDoc = 100;
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < docsPerCollection; i++)
                    {
                        var user = new User {Name = "Graziano " + i};
                        session.Store(user);
                        var metadata = session.Advanced.GetMetadataFor(user);
                        metadata[Constants.Documents.Metadata.Archive] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

                        var company = new Company {Name = "OG IT " + i};
                        session.Store(company);
                        metadata = session.Advanced.GetMetadataFor(company);
                        metadata[Constants.Documents.Metadata.Archive] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    }
                    session.SaveChanges();
                }

                for (int docNo = 1; docNo < docsPerCollection + 1; docNo++)
                {
                    store.Operations.Send(new CounterBatchOperation(new CounterBatch
                    {
                        Documents = new List<DocumentCountersOperation>
                        {
                            new DocumentCountersOperation
                            {
                                DocumentId = $"users/{docNo}-A",
                                Operations = Enumerable.Range(1, countersPerDoc)
                                    .Select(i => new CounterOperation
                                    {
                                        Type = CounterOperationType.Increment,
                                        CounterName = "c" + i,
                                        Delta = i * 10
                                    })
                                    .ToList()
                            },
                            new DocumentCountersOperation
                            {
                                DocumentId = $"users/{docNo}-A",
                                Operations = Enumerable.Range(1, countersPerDoc)
                                    .Select(i => new CounterOperation
                                    {
                                        Type = CounterOperationType.Increment,
                                        CounterName = "D" + i,
                                        Delta = i * 1000
                                    })
                                    .ToList()
                            },
                            new DocumentCountersOperation
                            {
                                DocumentId = $"companies/{docNo}-A",
                                Operations = Enumerable.Range(1, countersPerDoc)
                                    .Select(i => new CounterOperation
                                    {
                                        Type = CounterOperationType.Increment,
                                        CounterName = "og" + i,
                                        Delta = i * 100
                                    })
                                    .ToList()
                            }
                        }
                    }));
                }
                await SetupArchival(store);
                                
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DocumentsArchivist;
                await documentsArchiver.ArchiveDocs();
                for (int docId = 1; docId < docsPerCollection+1; docId++)
                {
                    using (var session = store.OpenSession())
                    {
                        var user = session.Load<User>($"users/{docId}-A");
                        var company = session.Load<User>($"companies/{docId}-A");
                        var metadataUser = session.Advanced.GetMetadataFor(user);
                        var metadataCompany = session.Advanced.GetMetadataFor(company);
                        Assert.Equal("UsersArchived", metadataUser[Constants.Documents.Metadata.Collection]);
                        Assert.Equal("CompaniesArchived", metadataCompany[Constants.Documents.Metadata.Collection]);
                    }
                    var counterNamesUser = Enumerable.Range(1, countersPerDoc)
                        .Select(i => "c" + i)
                        .ToArray();
                    
                    var counterNamesUserUppercase = Enumerable.Range(1, countersPerDoc)
                        .Select(i => "D" + i)
                        .ToArray();
                    
                    
                    var counterNamesCompany = Enumerable.Range(1, countersPerDoc)
                        .Select(i => "og" + i)
                        .ToArray();

                    var countersUser = store.Operations
                        .Send(new GetCountersOperation($"users/{docId}-A", counterNamesUser))
                        .Counters;
                    
                    var countersUserUppercase = store.Operations
                        .Send(new GetCountersOperation($"users/{docId}-A", counterNamesUserUppercase))
                        .Counters;

                    var countersCompany = store.Operations
                        .Send(new GetCountersOperation($"companies/{docId}-A", counterNamesCompany))
                        .Counters;

                    for (int i = 1; i <= countersPerDoc; i++)
                    {
                        var valUser = countersUser.FirstOrDefault(c => c.CounterName == "c" + i)?.TotalValue;
                        var valUppercaseUser = countersUserUppercase.FirstOrDefault(c => c.CounterName == "D" + i)?.TotalValue;
                        var valComp = countersCompany.FirstOrDefault(c => c.CounterName == "og" + i)?.TotalValue;
                        Assert.Equal(i*10, valUser);
                        Assert.Equal(i*1000, valUppercaseUser);
                        Assert.Equal(i*100, valComp);
                    }
                    
                    database = await GetDatabase(store.Database);
                    using (var ctx = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        ctx.OpenReadTransaction();
                        
                        // two splitted counter groups per doc
                        var usersArchivedCountersTable = database.DocumentsStorage.CountersStorage.GetCountersTable(ctx.Transaction.InnerTransaction, new CollectionName("UsersArchived"));
                        var companiesArchivedCountersTable = database.DocumentsStorage.CountersStorage.GetCountersTable(ctx.Transaction.InnerTransaction, new CollectionName("CompaniesArchived"));
                        Assert.Equal(docsPerCollection  * 2, usersArchivedCountersTable.NumberOfEntries);
                        Assert.Equal(docsPerCollection * 2 ,companiesArchivedCountersTable.NumberOfEntries);
                        
                        var archivedUsersCounters = database.DocumentsStorage.CountersStorage.GetCountersFrom(ctx, "UsersArchived", 0, 0, int.MaxValue).ToList();
                        var archivedCompanyCounters = database.DocumentsStorage.CountersStorage.GetCountersFrom(ctx, "CompaniesArchived", 0, 0, int.MaxValue).ToList();
                        Assert.Equal(docsPerCollection * 2,archivedUsersCounters.Count);
                        Assert.Equal(docsPerCollection * 2, archivedCompanyCounters.Count);
                    }   
                    
                }
            }
        }
    }
}
