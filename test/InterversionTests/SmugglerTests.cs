using System;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Smuggler.Migration;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class SmugglerTests : InterversionTestBase
    {
        const string Server42Version = "4.2.102-nightly-20200415-0501";
        readonly TimeSpan _operationTimeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromMinutes(1);
        
        public enum ExcludeOn
        {
            Non,
            Export,
            Import
        }
        
        public SmugglerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(ExcludeOn.Non)]
        [InlineData(ExcludeOn.Export)]
        [InlineData(ExcludeOn.Import)]
        public async Task CanExportFrom42AndImportToCurrent(ExcludeOn excludeOn)
        {
            var file = GetTempFileName();
            using var store42 = await GetDocumentStoreAsync(Server42Version);
            using var storeCurrent = GetDocumentStore();

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
            if (excludeOn == ExcludeOn.Export)
                exportOptions.OperateOnTypes &= ~(DatabaseItemType.Attachments | DatabaseItemType.RevisionDocuments | DatabaseItemType.CounterGroups);
            var exportOperation = await store42.Smuggler.ExportAsync(exportOptions, file);
            await exportOperation.WaitForCompletionAsync(_operationTimeout);

            var expected = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Import
            var importOptions = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };
            if (excludeOn == ExcludeOn.Import)
                importOptions.OperateOnTypes &= ~(DatabaseItemType.Attachments | DatabaseItemType.RevisionDocuments | DatabaseItemType.CounterGroups);
            var importOperation = await storeCurrent.Smuggler.ImportAsync(importOptions, file);
            await importOperation.WaitForCompletionAsync(_operationTimeout);

            var actual = await storeCurrent.Maintenance.SendAsync(new GetStatisticsOperation());

            //Assert
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);

            var export = await GetMetadataCounts(store42);
            var import = await GetMetadataCounts(storeCurrent);
            if (excludeOn == ExcludeOn.Non)
            {
                Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
                Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
                Assert.Equal(expected.CountOfCounterEntries, actual.CountOfCounterEntries);

                Assert.Equal(export, import);
            }
            else
            {
                Assert.Equal(0, actual.CountOfAttachments);
                Assert.Equal(0, actual.CountOfRevisionDocuments);
                Assert.Equal(0, actual.CountOfCounterEntries);

                Assert.Equal((0,0,0), import);
            }
        }

        [Theory]
        [InlineData(ExcludeOn.Non)]
        [InlineData(ExcludeOn.Export)]
        [InlineData(ExcludeOn.Import)]
        public async Task CanExportFromCurrentAndImportTo42(ExcludeOn excludeOn)
        {
            var file = GetTempFileName();
            using var store42 = await GetDocumentStoreAsync(Server42Version);
            using var storeCurrent = GetDocumentStore();
            //Export
            storeCurrent.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = storeCurrent.OpenAsyncSession())
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

            var exportOptions = new DatabaseSmugglerExportOptions();
            if (excludeOn == ExcludeOn.Export)
                exportOptions.OperateOnTypes &= ~(DatabaseItemType.Attachments | DatabaseItemType.RevisionDocuments | DatabaseItemType.CounterGroups);
            var exportOperation = await storeCurrent.Smuggler.ExportAsync(exportOptions, file);

            await exportOperation.WaitForCompletionAsync(_operationTimeout);

            DatabaseStatistics expected = await storeCurrent.Maintenance.SendAsync(new GetStatisticsOperation());

            //Import
            var importOptions = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };

            if (excludeOn == ExcludeOn.Import)
                importOptions.OperateOnTypes &= ~(DatabaseItemType.Attachments | DatabaseItemType.RevisionDocuments | DatabaseItemType.CounterGroups);
            var importOperation = await store42.Smuggler.ImportAsync(importOptions, file);
            await importOperation.WaitForCompletionAsync(_operationTimeout);

            var actual = await store42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Assert
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);

            var export = await GetMetadataCounts(storeCurrent);
            var import = await GetMetadataCounts(store42);
            if (excludeOn == ExcludeOn.Non)
            {
                Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
                Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
                Assert.Equal(expected.CountOfCounterEntries, actual.CountOfCounterEntries);

                Assert.Equal(export, import);
            }
            else
            {
                Assert.Equal(0, actual.CountOfAttachments);
                Assert.Equal(0, actual.CountOfRevisionDocuments);
                Assert.Equal(0, actual.CountOfCounterEntries);

                Assert.Equal((0,0,0), import);
            }
        }

        [Theory]
        [InlineData(ExcludeOn.Non)]
        [InlineData(ExcludeOn.Export)]
        [InlineData(ExcludeOn.Import)]
        public async Task CanExportAndImportClientCurrentServer42(ExcludeOn excludeOn)
        {
            var file = GetTempFileName();
            using var exportStore42 = await GetDocumentStoreAsync(Server42Version);

            exportStore42.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = exportStore42.OpenAsyncSession())
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
            if (excludeOn == ExcludeOn.Export)
                exportOptions.OperateOnTypes &= ~(DatabaseItemType.Attachments | DatabaseItemType.RevisionDocuments | DatabaseItemType.CounterGroups);
            var exportOperation = await exportStore42.Smuggler.ExportAsync(exportOptions, file);
            await exportOperation.WaitForCompletionAsync(_operationTimeout);

            var expected = await exportStore42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Import
            var databaseName = "Import" + exportStore42.Database;
            exportStore42.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName)));
            using var importStore42 = new DocumentStore {Database = databaseName, Urls = exportStore42.Urls}.Initialize();
            
            var importOptions = new DatabaseSmugglerImportOptions { SkipRevisionCreation = true };
            importOptions.OperateOnTypes &= ~DatabaseItemType.TimeSeries;
            if (excludeOn == ExcludeOn.Import)
                importOptions.OperateOnTypes &= ~(DatabaseItemType.Attachments | DatabaseItemType.RevisionDocuments | DatabaseItemType.CounterGroups);
            var importOperation = await importStore42.Smuggler.ImportAsync(importOptions, file);
            await importOperation.WaitForCompletionAsync(_operationTimeout);

            var actual = await importStore42.Maintenance.SendAsync(new GetStatisticsOperation());

            //Assert
            Assert.Equal(expected.CountOfIndexes, actual.CountOfIndexes);
            Assert.Equal(expected.CountOfDocuments, actual.CountOfDocuments);

            var export = await GetMetadataCounts(exportStore42);
            var import = await GetMetadataCounts(importStore42);
            if (excludeOn == ExcludeOn.Non)
            {
                Assert.Equal(expected.CountOfAttachments, actual.CountOfAttachments);
                Assert.Equal(expected.CountOfRevisionDocuments, actual.CountOfRevisionDocuments);
                Assert.Equal(expected.CountOfCounterEntries, actual.CountOfCounterEntries);

                Assert.Equal(export, import);
            }
            else
            {
                Assert.Equal(0, actual.CountOfAttachments);
                Assert.Equal(0, actual.CountOfRevisionDocuments);
                Assert.Equal(0, actual.CountOfCounterEntries);

                Assert.Equal((0,0,0), import);
            }
        }

        //Migrator
        [Fact]
        public async Task CanMigrateFrom42ToCurrent()
        {
            using var store42 = await GetDocumentStoreAsync(Server42Version);
            using var storeCurrent = GetDocumentStore();

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

            var operation = await Migrate(store42, storeCurrent);
            await operation.WaitForCompletionAsync(_operationTimeout);

            var fromStat = await store42.Maintenance.SendAsync(new GetStatisticsOperation());
            var toStat = await storeCurrent.Maintenance.SendAsync(new GetStatisticsOperation());
            
            //Assert
            Assert.Equal(fromStat.CountOfIndexes, toStat.CountOfIndexes);
            Assert.True(fromStat.CountOfDocuments < toStat.CountOfDocuments, 
                $"The count of document in target server should be at least the count of the source. source({fromStat.CountOfDocuments}) target({toStat.CountOfDocuments})");

            Assert.Equal(fromStat.CountOfAttachments, toStat.CountOfAttachments);
            Assert.Equal(fromStat.CountOfCounterEntries, toStat.CountOfCounterEntries);

            var fromMetadataCount = await GetMetadataCounts(store42);
            var toMetadataCount = await GetMetadataCounts(storeCurrent);
            Assert.Equal(fromMetadataCount, toMetadataCount);
        }

        [Fact]
        public async Task CanMigrateFromCurrentTo42()
        {
            using var store42 = await GetDocumentStoreAsync(Server42Version);
            using var storeCurrent = GetDocumentStore();

            storeCurrent.Maintenance.Send(new CreateSampleDataOperation());
            using (var session = storeCurrent.OpenAsyncSession())
            {
                for (var i = 0; i < 5; i++)
                {
                    var user = new User { Name = "raven" + i };
                    await session.StoreAsync(user);
                    session.CountersFor(user).Increment("Like");
                }
                DateTime dateTime = new DateTime(2020, 4, 12);
                for (var i = 0; i < 5; i++)
                {
                    var user = new User { Name = "raven" + i };
                    await session.StoreAsync(user);
                    session.TimeSeriesFor(user, "Heartrate").Append(dateTime, 59d, "watches/fitbit");
                }
                await session.SaveChangesAsync();
            }

            var operation = await Migrate(storeCurrent, store42, DatabaseItemType.TimeSeries);
            await operation.WaitForCompletionAsync(_operationTimeout);

            var fromStat = await storeCurrent.Maintenance.SendAsync(new GetStatisticsOperation());
            var toStat = await store42.Maintenance.SendAsync(new GetStatisticsOperation());
            
            //Assert
            Assert.Equal(fromStat.CountOfIndexes, toStat.CountOfIndexes);
            Assert.True(fromStat.CountOfDocuments < toStat.CountOfDocuments, 
                $"The count of document in target server should be at least the count of the source. source({fromStat.CountOfDocuments}) target({toStat.CountOfDocuments})");

            Assert.Equal(fromStat.CountOfAttachments, toStat.CountOfAttachments);
            Assert.Equal(fromStat.CountOfCounterEntries, toStat.CountOfCounterEntries);

            var fromMetadataCount = await GetMetadataCounts(storeCurrent);
            var toMetadataCount = await GetMetadataCounts(store42);
            Assert.Equal(fromMetadataCount, toMetadataCount);
        }

        
        private static async Task<Operation> Migrate(DocumentStore @from, DocumentStore to, DatabaseItemType exclude = DatabaseItemType.None)
        {
            using var client = new HttpClient();
            var url = $"{to.Urls.First()}/admin/remote-server/build/version?serverUrl={@from.Urls.First()}";
            var rawVersionRespond = (await client.GetAsync(url)).Content.ReadAsStringAsync().Result;
            var versionRespond = JsonConvert.DeserializeObject<BuildInfo>(rawVersionRespond);

            var configuration = new SingleDatabaseMigrationConfiguration
            {
                ServerUrl = @from.Urls.First(),
                BuildVersion = versionRespond.BuildVersion,
                BuildMajorVersion = versionRespond.MajorVersion,
                MigrationSettings = new DatabaseMigrationSettings
                {
                    DatabaseName = @from.Database, OperateOnTypes = DatabaseSmugglerOptions.DefaultOperateOnTypes & ~exclude
                }
            };

            var serializeObject = JsonConvert.SerializeObject(configuration, new StringEnumConverter());
            var data = new StringContent(serializeObject, Encoding.UTF8, "application/json");
            var rawOperationIdResult = await client.PostAsync($"{to.Urls.First()}/databases/{to.Database}/admin/smuggler/migrate/ravendb", data);
            var operationIdResult = JsonConvert.DeserializeObject<OperationIdResult>(rawOperationIdResult.Content.ReadAsStringAsync().Result);

            return new Operation(to.GetRequestExecutor(), () => to.Changes(), to.Conventions, operationIdResult.OperationId);
        }

        private static async Task<(int Counter, int Attachment, int Revision)> GetMetadataCounts(IDocumentStore importStore42)
        {
            using (var session = importStore42.OpenAsyncSession())
            {
                var allDoc = await session.Advanced.AsyncRawQuery<dynamic>("from @all_docs").ToArrayAsync();
                var metadatas = allDoc.Select(session.Advanced.GetMetadataFor).ToArray();
                var counters = metadatas.Count(md => md.TryGetValue(Constants.Documents.Metadata.Flags, out string f) && f.Contains(nameof(DocumentFlags.HasCounters)) );
                var attachment = metadatas.Count(md => md.TryGetValue(Constants.Documents.Metadata.Flags, out string f) && f.Contains(nameof(DocumentFlags.HasAttachments)) );
                var revision = metadatas.Count(md => md.TryGetValue(Constants.Documents.Metadata.Flags, out string f) && f.Contains(nameof(DocumentFlags.HasRevisions)) );
                return (counters, attachment, revision);
            }
        }
    }
}
