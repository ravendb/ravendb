using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Remote;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.ETL.Olap;
using Sparrow.Json;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;

namespace InterversionTests
{
    public class AttachmentsTests : InterversionTestBase
    {
        public AttachmentsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Attachments | RavenTestCategory.Replication, RavenPlatform.Windows | RavenPlatform.Linux, AwsRequired = true)]
        public async Task CannotReplicateRemoteAttachmentsToOld()
        {
            await CannotReplicateRemoteAttachmentsToOldInternal(InterversionTestOptions.Default);
        }

        private async Task CannotReplicateRemoteAttachmentsToOldInternal(InterversionTestOptions ops)
        {
            var version = Server62Version;

            var settings = Etl.GetS3Settings(nameof(AttachmentsTests), $"{Guid.NewGuid()}").ToRemoteAttachmentsS3Settings();

            await using (DeleteObjects(settings))
            using (var oldStore = await GetDocumentStoreAsync(version, ops))
            using (var store = GetDocumentStore())
            {
                await CannotRemoteAttachmentsToOldInternal(settings, store);

                await SetupReplicationAsync(store, oldStore);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.True(await WaitForValueAsync(() =>
                {
                    var replicationLoader = db.ReplicationLoader;
                    return replicationLoader.OutgoingFailureInfo.Count > 0;
                }, true, interval: 333));

                var replicationLoader = db.ReplicationLoader;
                Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
                Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true), "WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true)");
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))), "replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException)))");
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("found an item of type 'AttachmentReplicationItem' to replicate"))), "replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains('found an item of type 'AttachmentReplicationItem' to replicate')))");
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Attachments | RavenTestCategory.Replication, RavenPlatform.Windows | RavenPlatform.Linux, AwsRequired = true)]
        public async Task CannotReplicateRemoteAttachmentsToOldSharded()
        {
            await CannotReplicateRemoteAttachmentsToOldInternal(InterversionTestOptions.Sharded);
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Attachments | RavenTestCategory.Replication, RavenPlatform.Windows | RavenPlatform.Linux, AwsRequired = true)]
        public async Task CannotEtlRemoteAttachmentsToOld()
        {
            var version = Server62Version;

            var settings = Etl.GetS3Settings(nameof(AttachmentsTests), $"{Guid.NewGuid()}").ToRemoteAttachmentsS3Settings();

            await using (DeleteObjects(settings))
            using (var oldStore = await GetDocumentStoreAsync(version))
            using (var store = GetDocumentStore())
            {
                await CannotRemoteAttachmentsToOldInternal(settings, store);

                var taskName = "etl-test";
                var csName = "cs-test";
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = csName,
                    Name = taskName,
                    Transforms = { new Transformation { Name = "S1", Collections = { "Orders" } } }
                };

                var connectionString = new RavenConnectionString { Name = csName, TopologyDiscoveryUrls = oldStore.Urls, Database = oldStore.Database, };

                Etl.AddEtl(store, configuration, connectionString);

                IEnumerable<TaskProcessErrorTableValue> errors = null;
                Assert.True(await WaitForValueAsync(async () =>
                {
                    errors = await Etl.GetProcessLoadErrorsAsync(store.Database, configuration);

                    return errors.Any();
                }, true, 60_000, interval: 322));

                Assert.NotEmpty(errors);
                
                var error = errors.First();
                Assert.NotNull(error.CreatedAt);
                Assert.NotNull(error.Error);
                Assert.Contains("System.NullReferenceException: System.NullReferenceException: Object reference not set to an instance of an object.", error.Error);
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion | RavenTestCategory.Attachments | RavenTestCategory.Replication, RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData(Server54Version)]
        [InlineData(Server62Version)]
        public async Task CanReplicateRegularAttachmentsToOlderVersions(string olderVersion)
        {
            using (var sourceStore = GetDocumentStore())
            using (var destinationStore = await GetDocumentStoreAsync(olderVersion))
            {
                string docId = CreateRegularAttachment(sourceStore, out string attachmentName, out byte[] attachmentContent, out string contentType);

                // Setup replication from source to destination
                await SetupReplicationAsync(sourceStore, destinationStore);

                // Wait for replication to complete
                await EnsureReplicatingAsync(sourceStore, destinationStore);

                // Verify document was replicated
                using (var session = destinationStore.OpenSession())
                {
                    var document = session.Load<Order>(docId);
                    Assert.NotNull(document);
                }

                // Verify attachment was replicated to the older version
                using (var session = destinationStore.OpenSession())
                {
                    var attachmentExists = session.Advanced.Attachments.Exists(docId, attachmentName);
                    Assert.True(attachmentExists);

                    // Verify attachment content
                    using var attachmentResult = session.Advanced.Attachments.Get(docId, attachmentName);
                    Assert.NotNull(attachmentResult);
                    Assert.Equal(attachmentName, attachmentResult.Details.Name);
                    Assert.Equal(contentType, attachmentResult.Details.ContentType);
                    Assert.Equal(attachmentContent.Length, attachmentResult.Details.Size);

                    using var resultStream = new MemoryStream();
                    await attachmentResult.Stream.CopyToAsync(resultStream);
                    var replicatedContent = resultStream.ToArray();
                    Assert.Equal(attachmentContent, replicatedContent);
                }
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion | RavenTestCategory.Attachments | RavenTestCategory.Etl, RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData(Server54Version)]
        [InlineData(Server62Version)]
        public async Task CanEtlRegularAttachmentsToOlderVersions(string olderVersion)
        {
            using (var sourceStore = GetDocumentStore())
            using (var destinationStore = await GetDocumentStoreAsync(olderVersion))
            {
                string docId = CreateRegularAttachment(sourceStore, out string attachmentName, out byte[] attachmentContent, out string contentType);

                // Setup ETL from source to destination
                var taskName = "etl-test";
                var csName = "cs-test";
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = csName,
                    Name = taskName,
                    Transforms = { new Transformation { Name = "S1", Collections = { "Orders" } } }
                };

                var connectionString = new RavenConnectionString
                {
                    Name = csName,
                    TopologyDiscoveryUrls = destinationStore.Urls,
                    Database = destinationStore.Database
                };

                Etl.AddEtl(sourceStore, configuration, connectionString);

                // Wait for ETL to complete
                var etlDone = Etl.WaitForEtlToComplete(sourceStore);
                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));
                // Verify document was ETL'd to the older version
                using (var session = destinationStore.OpenSession())
                {
                    var document = session.Load<Order>(docId);
                    Assert.NotNull(document);
                }

                // Verify attachment was ETL'd to the older version
                using (var session = destinationStore.OpenSession())
                {
                    var attachmentExists = session.Advanced.Attachments.Exists(docId, attachmentName);
                    Assert.True(attachmentExists);

                    // Verify attachment content
                    using var attachmentResult = session.Advanced.Attachments.Get(docId, attachmentName);
                    Assert.NotNull(attachmentResult);
                    Assert.Equal(attachmentName, attachmentResult.Details.Name);
                    Assert.Equal(contentType, attachmentResult.Details.ContentType);
                    Assert.Equal(attachmentContent.Length, attachmentResult.Details.Size);

                    using var resultStream = new MemoryStream();
                    await attachmentResult.Stream.CopyToAsync(resultStream);
                    var etlContent = resultStream.ToArray();
                    Assert.Equal(attachmentContent, etlContent);
                }

                // Verify no ETL errors occurred
                var etlErrors = await Etl.GetItemLoadErrorsAsync(sourceStore.Database, configuration);
                Assert.Empty(etlErrors);
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion | RavenTestCategory.Attachments | RavenTestCategory.Smuggler, RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData(Server54Version, SmugglerTests._operateOnTypes54, SmugglerTests._operateOnRecordTypes54)]
        [InlineData(Server62Version, SmugglerTests._operateOnTypes62, SmugglerTests._operateOnRecordTypes62)]
        public async Task CanExportAttachmentsFromOlderVersionsAndImportToCurrent(string olderVersion, DatabaseItemType operateOnTypes, DatabaseRecordItemType operateOnRecordTypes)
        {
            var file = GetTempFileName();
            using (var oldStore = await GetDocumentStoreAsync(olderVersion))
            using (var currentStore = GetDocumentStore())
            {
                try
                {
                    // Create documents with attachments in the older version
                    var docIds = await CreateMultipleDocumentsWithAttachments(oldStore);

                    // Export from old version
                    var exportOptions = new DatabaseSmugglerExportOptions
                    {
                        OperateOnTypes = operateOnTypes,
                        OperateOnDatabaseRecordTypes = operateOnRecordTypes
                    };

                    var exportOperation = await oldStore.Smuggler.ExportAsync(exportOptions, file);
                    await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    // Import to current version
                    var importOptions = new DatabaseSmugglerImportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Attachments
                    };
                    var importOperation = await currentStore.Smuggler.ImportAsync(importOptions, file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    // Verify documents and attachments were imported correctly
                    await VerifyDocumentsAndAttachments(currentStore, docIds);

                    // Verify statistics match
                    var oldStats = await oldStore.Maintenance.SendAsync(new GetStatisticsOperation());
                    var currentStats = await currentStore.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(oldStats.CountOfDocuments, currentStats.CountOfDocuments);
                    Assert.Equal(oldStats.CountOfAttachments, currentStats.CountOfAttachments);
                }
                finally
                {
                    if (File.Exists(file))
                        File.Delete(file);
                }
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion | RavenTestCategory.Attachments | RavenTestCategory.Smuggler, RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData(Server54Version)]
        [InlineData(Server62Version)]
        public async Task CanExportAttachmentsFromCurrentAndImportToOlderVersions(string olderVersion)
        {
            var file = GetTempFileName();
            using (var currentStore = GetDocumentStore())
            using (var oldStore = await GetDocumentStoreAsync(olderVersion))
            {
                try
                {
                    // Create documents with attachments in the current version
                    var docIds = await CreateMultipleDocumentsWithAttachments(currentStore);

                    // Export from current version
                    var exportOptions = new DatabaseSmugglerExportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Attachments
                    };
                    var exportOperation = await currentStore.Smuggler.ExportAsync(exportOptions, file);
                    await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    // Import to older version
                    var importOptions = new DatabaseSmugglerImportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Attachments
                    };
                    var importOperation = await oldStore.Smuggler.ImportAsync(importOptions, file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    // Verify documents and attachments were imported correctly
                    await VerifyDocumentsAndAttachments(oldStore, docIds);

                    // Verify statistics match
                    var currentStats = await currentStore.Maintenance.SendAsync(new GetStatisticsOperation());
                    var oldStats = await oldStore.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(currentStats.CountOfDocuments, oldStats.CountOfDocuments);
                    Assert.Equal(currentStats.CountOfAttachments, oldStats.CountOfAttachments);
                }
                finally
                {
                    if (File.Exists(file))
                        File.Delete(file);
                }
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion | RavenTestCategory.Attachments, RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData(Server62Version)]
        public async Task ReplicationShouldSendMissingAttachments(string olderVersion)
        {
            using (var source = await GetDocumentStoreAsync(olderVersion))
            using (var destination = GetDocumentStore())
            {
                await SetupReplicationAsync(source, destination);

                using (var session = source.OpenAsyncSession())
                using (var fooStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    for (int i = 0; i < 25; i++)
                    {
                        fooStream.Position = 0;
                        await session.StoreAsync(new User { Name = "Foo" }, $"FoObAr/{i}");
                        session.Advanced.Attachments.Store($"FoObAr/{i}", "foo.png", fooStream, "image/png");
                        await session.SaveChangesAsync();

                        Assert.NotNull(WaitForDocumentToReplicate<User>(destination, $"FoObAr/{i}", 15 * 1000));
                    }
                }

                using (var session = destination.OpenAsyncSession())
                {
                    for (int i = 0; i < 25; i++)
                    {
                        session.Delete($"FoObAr/{i}");
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = source.OpenAsyncSession())
                using (var fooStream2 = new MemoryStream(new byte[] { 4, 5, 6 }))
                {
                    for (int i = 0; i < 25; i++)
                    {
                        fooStream2.Position = 0;
                        session.Advanced.Attachments.Store($"FoObAr/{i}", "foo2.png", fooStream2, "image/png");
                        await session.SaveChangesAsync();

                        Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, $"FoObAr/{i}", "foo2.png", 30 * 1000));
                    }
                }

                var buffer = new byte[3];
                using (var session = destination.OpenAsyncSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
                    for (int i = 0; i < 25; i++)
                    {
                        var user = await session.LoadAsync<User>($"FoObAr/{i}");
                        var attachments = session.Advanced.Attachments.GetNames(user);
                        Assert.Equal(2, attachments.Length);

                        foreach (var name in attachments)
                        {
                            using (var attachment = await session.Advanced.Attachments.GetAsync(user, name.Name))
                            {
                                Assert.NotNull(attachment);
                                Assert.Equal(3, await attachment.Stream.ReadAsync(buffer, 0, 3));
                                if (attachment.Details.Name == "foo.png")
                                {
                                    Assert.Equal(1, buffer[0]);
                                    Assert.Equal(2, buffer[1]);
                                    Assert.Equal(3, buffer[2]);
                                }
                            }
                        }
                    }
                }
            }
        }

        private static async Task<List<string>> CreateMultipleDocumentsWithAttachments(DocumentStore store)
        {
            var docIds = new List<string>();

            // Create 3 documents with different types of attachments
            for (int i = 1; i <= 3; i++)
            {
                var docId = $"orders/{i}";
                docIds.Add(docId);

                // Store document
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Id = docId,
                        OrderedAt = new DateTime(2024, 1, i),
                        Company = $"Companies/{i}",
                        ShipVia = $"Shippers/{i}"
                    });
                    await session.SaveChangesAsync();
                }

                // Store multiple attachments per document
                using (var session = store.OpenAsyncSession())
                {
                    // Text attachment
                    var textContent = Encoding.UTF8.GetBytes($"This is a text file for order {i}");
                    using var textStream = new MemoryStream(textContent);
                    session.Advanced.Attachments.Store(docId, $"readme-{i}.txt", textStream, "text/plain");

                    // Image attachment
                    var imageContent = new byte[] { 0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46 }; // JPEG header
                    using var imageStream = new MemoryStream(imageContent);
                    session.Advanced.Attachments.Store(docId, $"image-{i}.jpg", imageStream, "image/jpeg");

                    // Binary attachment
                    var binaryContent = new byte[1024];
                    new Random(i).NextBytes(binaryContent);
                    using var binaryStream = new MemoryStream(binaryContent);
                    session.Advanced.Attachments.Store(docId, $"data-{i}.bin", binaryStream, "application/octet-stream");

                    await session.SaveChangesAsync();
                }
            }

            return docIds;
        }

        private static async Task VerifyDocumentsAndAttachments(DocumentStore store, List<string> expectedDocIds)
        {
            using (var session = store.OpenSession())
            {
                foreach (var docId in expectedDocIds)
                {
                    // Verify document exists
                    var document = session.Load<Order>(docId);
                    Assert.NotNull(document);

                    // Extract the number from docId (e.g., "orders/1" -> 1)
                    var docNumber = int.Parse(docId.Split('/')[1]);

                    // Verify all three attachments exist
                    var textAttachmentName = $"readme-{docNumber}.txt";
                    var imageAttachmentName = $"image-{docNumber}.jpg";
                    var binaryAttachmentName = $"data-{docNumber}.bin";

                    Assert.True(session.Advanced.Attachments.Exists(docId, textAttachmentName));
                    Assert.True(session.Advanced.Attachments.Exists(docId, imageAttachmentName));
                    Assert.True(session.Advanced.Attachments.Exists(docId, binaryAttachmentName));

                    // Verify text attachment content
                    using (var textResult = session.Advanced.Attachments.Get(docId, textAttachmentName))
                    {
                        Assert.NotNull(textResult);
                        Assert.Equal("text/plain", textResult.Details.ContentType);

                        using var resultStream = new MemoryStream();
                        await textResult.Stream.CopyToAsync(resultStream);
                        var content = Encoding.UTF8.GetString(resultStream.ToArray());
                        Assert.Equal($"This is a text file for order {docNumber}", content);
                    }

                    // Verify image attachment properties
                    using (var imageResult = session.Advanced.Attachments.Get(docId, imageAttachmentName))
                    {
                        Assert.NotNull(imageResult);
                        Assert.Equal("image/jpeg", imageResult.Details.ContentType);
                        Assert.Equal(10, imageResult.Details.Size); // JPEG header size
                    }

                    // Verify binary attachment properties  
                    using (var binaryResult = session.Advanced.Attachments.Get(docId, binaryAttachmentName))
                    {
                        Assert.NotNull(binaryResult);
                        Assert.Equal("application/octet-stream", binaryResult.Details.ContentType);
                        Assert.Equal(1024, binaryResult.Details.Size);
                    }
                }
            }
        }

        private static string CreateRegularAttachment(DocumentStore sourceStore, out string attachmentName, out byte[] attachmentContent, out string contentType)
        {
            // Create a document with a regular attachment in the source store
            var docId = "orders/1";
            attachmentName = "test-attachment.png";
            attachmentContent = new byte[] { 1, 2, 3, 4, 5 };
            contentType = "image/png";

            // Store document
            using (var session = sourceStore.OpenSession())
            {
                session.Store(new Order { Id = docId, OrderedAt = new DateTime(2024, 1, 1), Company = "Companies/1" });
                session.SaveChanges();
            }

            // Store regular attachment (not remote)
            using (var session = sourceStore.OpenSession())
            {
                using var attachmentStream = new MemoryStream(attachmentContent);
                session.Advanced.Attachments.Store(docId, attachmentName, attachmentStream, contentType);
                session.SaveChanges();
            }

            // Verify attachment exists in source
            using (var session = sourceStore.OpenSession())
            {
                var exists = session.Advanced.Attachments.Exists(docId, attachmentName);
                Assert.True(exists);
            }

            return docId;
        }

        /// <summary>
        /// Operation to get debug statistics for ETL processes.
        /// </summary>
        internal sealed class GetEtlDebugStatsOperation : IMaintenanceOperation<string>
        {
            private readonly string[] _etlTaskNames;

            /// <summary>
            /// Initialize operation to get debug stats for all ETL tasks.
            /// </summary>
            public GetEtlDebugStatsOperation()
            {
                _etlTaskNames = null;
            }

            /// <summary>
            /// Initialize operation to get debug stats for specific ETL tasks.
            /// </summary>
            /// <param name="etlTaskNames">Names of the ETL tasks to get debug stats for. If null or empty, gets stats for all ETL tasks.</param>
            public GetEtlDebugStatsOperation(params string[] etlTaskNames)
            {
                _etlTaskNames = etlTaskNames;
            }

            public RavenCommand<string> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetEtlDebugStatsCommand(_etlTaskNames);
            }

            private sealed class GetEtlDebugStatsCommand : RavenCommand<string>
            {
                private readonly string[] _etlTaskNames;

                public GetEtlDebugStatsCommand(string[] etlTaskNames)
                {
                    _etlTaskNames = etlTaskNames;
                }

                public override bool IsReadRequest => true;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/etl/debug/stats";

                    if (_etlTaskNames is { Length: > 0 })
                    {
                        for (var i = 0; i < _etlTaskNames.Length; i++)
                            url += $"{(i == 0 ? "?" : "&")}name={Uri.EscapeDataString(_etlTaskNames[i])}";
                    }

                    return new HttpRequestMessage { Method = HttpMethod.Get };
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        return;

                    Result = response.ToString();
                }
            }
        }
        private async Task CannotRemoteAttachmentsToOldInternal(RemoteAttachmentsS3Settings settings, DocumentStore store)
        {
            string identifier = await CreateRemoteAttachmentsConfigurationAndGetIdentifier(settings, store);

            var id = "Orders/1";
            using (var session = store.OpenSession())
            {
                session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/1", Company = $"Companies/1" });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                {
                    RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                    ContentType = "image/png"
                });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var exists = session.Advanced.Attachments.Exists(id, "test.png");
                Assert.True(exists);
            }

            await RemoteAndAssertCount(store, settings, 1);

            using (var session = store.OpenSession())
            {
                var remoteExists = session.Advanced.Attachments.Exists(id, "test.png");
                Assert.True(remoteExists);
            }
        }

        private IAsyncDisposable DeleteObjects(IS3Settings settings)
        {
            return new AsyncDisposableAction(async () =>
            {

                if (settings == null)
                    return;

                await S3TestsHelper.DeleteObjects(settings, prefix: $"{settings.RemoteFolderName}", delimiter: string.Empty);
            });
        }

        private static async Task<string> CreateRemoteAttachmentsConfigurationAndGetIdentifier(RemoteAttachmentsS3Settings settings, DocumentStore store)
        {
            var identifier = "conf-identifier-s3";
            var config = new RemoteAttachmentsConfiguration()
            {
                Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                {
                    {
                        identifier, new RemoteAttachmentsDestinationConfiguration()
                        {
                            S3Settings = settings,
                            Disabled = false,
                        }
                    }
                },
                CheckFrequencyInSec = 1000
            };

            await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(config));
            return identifier;
        }

        private async Task RemoteAndAssertCount(DocumentStore store, IS3Settings settings, int expected)
        {
            var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);


            List<S3FileInfoDetails> cloudObjects = null;
            var val3 = await WaitForValueAsync(async () =>
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
                using (var s3Client = new RavenAwsS3Client(settings, RavenTestBase.EtlTestBase.DefaultBackupConfiguration, cancellationToken: cts.Token))
                {
                    var prefix = $"{settings.RemoteFolderName}";
                    cloudObjects = await s3Client.ListAllObjectsAsync(prefix, string.Empty, false);
                    return cloudObjects.Count;
                }
            }, expected);
            Assert.Equal(expected, val3);

            var x123 = cloudObjects.Select(x => new FileInfoDetails()
            {
                FullPath = x.FullPath,
                LastModified = x.LastModified
            }).ToList();

            Assert.Equal(expected, x123.Count);
        }

        private class FileInfoDetails
        {
            public string FullPath { get; set; }

            public DateTime LastModified { get; set; }
        }
    }
}
