using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class SmugglerTests : InterversionTestBase
    {
        //TODO Need to be changed to version with relevant fix
        const string Server42Version = "4.2.101";
        
        public SmugglerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanExportFrom42AndImportTo5()
        {
            var file = GetTempFileName();
            using var store42 = await GetDocumentStoreAsync(Server42Version);
            using var store5 = GetDocumentStore();

            store42.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = store42.OpenAsyncSession())
            {
                for (var i = 0; i < 5; i++)
                {
                    var user = new User { Name = "raven" + i };
                    await session.StoreAsync(user);
                    session.CountersFor(user).Increment("Like");
                }
                await session.SaveChangesAsync();
            }
            
            //Export
            var exportOptions = new DatabaseSmugglerExportOptions();
            exportOptions.OperateOnTypes &= ~DatabaseItemType.TimeSeries;
            
            var exportOperation = await store42.Smuggler.ExportAsync(exportOptions, file);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var expected = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Import
            var importOptions = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };
            
            var importOperation = await store5.Smuggler.ImportAsync(importOptions, file);
            await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var actual = await store5.Maintenance.SendAsync(new GetStatisticsOperation());

            //Assert
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);
            Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
        }
        
        [Fact]
        public async Task CanExportFrom5AndImportTo42()
        {
            var file = GetTempFileName();
            using var store42 = await GetDocumentStoreAsync(Server42Version);
            using var store5 = GetDocumentStore();
            //Export
            store5.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = store5.OpenAsyncSession())
            {
                var dateTime = new DateTime(2020, 3, 29);
                    
                for (var i = 0; i < 5; i++)
                {
                    var user = new User { Name = "raven" + i };
                    await session.StoreAsync(user);
                    session.TimeSeriesFor(user, "Heartrate").Append(dateTime, 59d, "watches/fitbit");
                }
                await session.SaveChangesAsync();
            }
            var exportOperation = await store5.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            DatabaseStatistics expected = await store5.Maintenance.SendAsync(new GetStatisticsOperation());
                    
            //Import
            var importOptions = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };
            importOptions.OperateOnTypes &= ~DatabaseItemType.TimeSeries;
            var importOperation = await store42.Smuggler.ImportAsync(importOptions, file);
            await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var actual = await store42.Maintenance.SendAsync(new GetStatisticsOperation());
            
            //Assert
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);
            Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
        }

        [Fact]
        public async Task CanExportAndImportClient5Server42()
        {
            var file = GetTempFileName();
            using var store42 = await GetDocumentStoreAsync(Server42Version);

            store42.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = store42.OpenAsyncSession())
            {
                for (var i = 0; i < 5; i++)
                {
                    var user = new User { Name = "raven" + i };
                    await session.StoreAsync(user);
                    session.CountersFor(user).Increment("Like");
                }
                await session.SaveChangesAsync();
            }
            
            //Export
            var exportOptions = new DatabaseSmugglerExportOptions();
            exportOptions.OperateOnTypes &= ~DatabaseItemType.TimeSeries;
            var exportOperation = await store42.Smuggler.ExportAsync(exportOptions, file);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var expected = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Import
            var importOptions = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };
            importOptions.OperateOnTypes &= ~DatabaseItemType.TimeSeries;
            
            var importOperation = await store42.Smuggler.ImportAsync(importOptions, file);
            await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var actual = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Assert
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);
            Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
        }
        
        const string Server5Version = "5.0.0-nightly-20200401-0544";
        
        [Fact]
        public async Task CanExportFrom42AndImportTo5()
        {
            var file = GetTempFileName();
            using var store5 = await GetDocumentStoreAsync(Server5Version);
            using var store42 = GetDocumentStore();

            store42.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = store42.OpenAsyncSession())
            {
                for (var i = 0; i < 5; i++)
                {
                    var user = new User { Name = "raven" + i };
                    await session.StoreAsync(user);
                    session.CountersFor(user).Increment("Like");
                }
                await session.SaveChangesAsync();
            }

            //Export
            var options = new DatabaseSmugglerExportOptions();

            var importOperation = await store42.Smuggler.ExportAsync(options, file);
            await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var expected = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Import
            var exportOperation = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };

            var operation = await store5.Smuggler.ImportAsync(exportOperation, file);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var actual = await store5.Maintenance.SendAsync(new GetStatisticsOperation());

            //Assert
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);
            Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
        }

        [Fact]
        public async Task CanExportFrom5AndImportTo42()
        {
            var file = GetTempFileName();
            using var store5 = await GetDocumentStoreAsync(Server5Version);
            using var store42 = GetDocumentStore();
            //Export
            store5.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = store5.OpenAsyncSession())
            {
                for (var i = 0; i < 5; i++)
                {
                    var user = new User { Name = "raven" + i };
                    await session.StoreAsync(user);
                    session.CountersFor(user).Increment("Like");
                }
                await session.SaveChangesAsync();
            }
            var exportOperation = await store5.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            DatabaseStatistics expected = await store5.Maintenance.SendAsync(new GetStatisticsOperation());

            //Import
            var options = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };
            var importOperation = await store42.Smuggler.ImportAsync(options, file);
            await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var actual = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Assert
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);
            Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
        }

        [Fact]
        public async Task CanExportAndImportClient42Server5()
        {
            var file = GetTempFileName();
            using var store5 = await GetDocumentStoreAsync(Server5Version);

            store5.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = store5.OpenAsyncSession())
            {
                for (var i = 0; i < 5; i++)
                {
                    var user = new User { Name = "raven" + i };
                    await session.StoreAsync(user);
                    session.CountersFor(user).Increment("Like");
                }
                await session.SaveChangesAsync();
            }

            //Export
            var options = new DatabaseSmugglerExportOptions();

            var importOperation = await store5.Smuggler.ExportAsync(options, file);
            await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var expected = await store5.Maintenance.SendAsync(new GetStatisticsOperation());

            //Import
            var exportOperation = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };

            var operation = await store5.Smuggler.ImportAsync(exportOperation, file);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var actual = await store5.Maintenance.SendAsync(new GetStatisticsOperation());

            //Assert
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);
            Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
        }
    }
}
