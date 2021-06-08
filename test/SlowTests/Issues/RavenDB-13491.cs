using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13491 : RavenTestBase
    {
        public RavenDB_13491(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Counters_export_should_respect_collection_selection_1()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/1");
                        await session.StoreAsync(new User(), "users/2");
                        await session.StoreAsync(new User(), "users/3");

                        await session.StoreAsync(new Order(), "orders/1");
                        await session.StoreAsync(new Order(), "orders/2");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("likes", 100);
                        session.CountersFor("users/1").Increment("dislikes", 200);
                        session.CountersFor("users/2").Increment("downloads", 500);
                        session.CountersFor("users/3").Increment("downloads", 500);

                        await session.SaveChangesAsync();
                    }

                    var exportOptions = new DatabaseSmugglerExportOptions
                    {
                        Collections = new List<string>
                        {
                            "Orders"
                        }
                    };

                    var operation = await store1.Smuggler.ExportAsync(exportOptions, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfCounterEntries);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task Counters_export_should_respect_collection_selection_2()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/1");
                        await session.StoreAsync(new User(), "users/2");
                        await session.StoreAsync(new User(), "users/3");


                        await session.StoreAsync(new Order(), "orders/1");
                        await session.StoreAsync(new Order(), "orders/2");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("likes", 100);
                        session.CountersFor("users/1").Increment("dislikes", 200);
                        session.CountersFor("users/2").Increment("downloads", 500);
                        session.CountersFor("users/3").Increment("downloads", 500);


                        session.CountersFor("orders/1").Increment("downloads", 100);
                        session.CountersFor("orders/2").Increment("downloads", 200);


                        await session.SaveChangesAsync();
                    }

                    var exportOptions = new DatabaseSmugglerExportOptions
                    {
                        Collections = new List<string>
                        {
                            "Orders"
                        }
                    };

                    var operation = await store1.Smuggler.ExportAsync(exportOptions, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(2, stats.CountOfCounterEntries);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var order1 = await session.LoadAsync<Order>("orders/1");
                        var counter = await session.CountersFor(order1).GetAsync("downloads");

                        Assert.Equal(100, counter);

                        var order2 = await session.LoadAsync<Order>("orders/2");
                        counter = await session.CountersFor(order2).GetAsync("downloads");

                        Assert.Equal(200, counter);

                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task Should_report_errors_on_attempt_to_import_counter_of_non_existing_document()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order(), "orders/1");
                        await session.StoreAsync(new Order(), "orders/2");

                        session.CountersFor("orders/1").Increment("downloads", 100);
                        session.CountersFor("orders/2").Increment("downloads", 200);

                        await session.SaveChangesAsync();
                    }

                    var exportOptions = new DatabaseSmugglerExportOptions();
                    exportOptions.OperateOnTypes &= ~DatabaseItemType.Documents;

                    var exportOperation = await store1.Smuggler.ExportAsync(exportOptions, file);
                    var exportResult = (SmugglerResult)exportOperation.WaitForCompletion();
                    var progress = (SmugglerResult.SmugglerProgress)exportResult.Progress;

                    Assert.Equal(0, progress.Documents.ReadCount);
                    Assert.Equal(2, progress.Counters.ReadCount);

                    var importOperation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    var importResult = (SmugglerResult)importOperation.WaitForCompletion();

                    progress = (SmugglerResult.SmugglerProgress)importResult.Progress;

                    Assert.Equal(0, progress.Documents.ReadCount);
                    Assert.Equal(2, progress.Counters.ReadCount);
                    Assert.Equal(2, progress.Counters.ErroredCount);

                    Assert.True(importResult.Messages.Any(message => 
                        message.Contains("Document 'orders/1' does not exist. Cannot operate on counters of a missing document")));
                    Assert.True(importResult.Messages.Any(message => 
                        message.Contains("Document 'orders/2' does not exist. Cannot operate on counters of a missing document")));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(0, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfCounterEntries);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

    }
}
