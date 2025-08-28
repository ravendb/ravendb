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
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using SlowTests.Server.Documents.Attachments;
using SlowTests.Server.Documents.ETL.Olap;
using Sparrow.Json;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class AttachmentsTests : InterversionTestBase
    {
        public AttachmentsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Attachments | RavenTestCategory.Replication, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task CannotReplicateRetiredAttachmentsToOld()
        {
            var version = Server62Version;

            var settings = Etl.GetS3Settings(nameof(AttachmentsTests), $"{Guid.NewGuid()}");

            await using (DeleteObjects(settings))
            using (var oldStore = await GetDocumentStoreAsync(version))
            using (var store = GetDocumentStore())
            {
                await CannotRetiredAttachmentsToOldInternal(settings, store);

                await SetupReplicationAsync(store, oldStore);

                var replicationLoader = (await Databases.GetDocumentDatabaseInstanceFor(store)).ReplicationLoader;
                Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
                Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true), "WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true)");
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))), "replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException)))");
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("found an item of type 'AttachmentReplicationItem' to replicate"))), "replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains('found an item of type 'AttachmentReplicationItem' to replicate')))");
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Attachments | RavenTestCategory.Replication, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task CannotEtlRetiredAttachmentsToOld()
        {
            var version = Server62Version;

            var settings = Etl.GetS3Settings(nameof(AttachmentsTests), $"{Guid.NewGuid()}");

            await using (DeleteObjects(settings))
            using (var oldStore = await GetDocumentStoreAsync(version))
            using (var store = GetDocumentStore())
            {
                await CannotRetiredAttachmentsToOldInternal(settings, store);

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

                AlertRaised alert = null;
                Assert.True(await WaitForValueAsync(async () =>
                {
                    var operation = new GetEtlDebugStatsOperation();
                    string statsJson = await store.Maintenance.SendAsync(operation);

                    alert = await GetAlertWithDetailsFromEtlFailure(statsJson);

                    if (alert == null)
                        return false;

                    return true;
                }, true, 60_000, interval: 322));

                Assert.NotNull(alert);

                Assert.Equal(AlertType.Etl_LoadError, alert.AlertType);
                Assert.NotNull(alert.Details);
                EtlErrorsDetails details = (EtlErrorsDetails)alert.Details;

                Assert.NotEmpty(details.Errors);
                var error = details.Errors.Dequeue();
                Assert.NotNull(error.Date);
                Assert.NotNull(error.Error);
                Assert.Contains("System.NullReferenceException: System.NullReferenceException: Object reference not set to an instance of an object.", error.Error);
            }
        }

        private static async Task<AlertRaised> GetAlertWithDetailsFromEtlFailure(string statsJson)
        {
            // Parse the JSON string to BlittableJsonReaderObject
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(statsJson)))
            using (context.GetMemoryBuffer(out var bytes))
            {
                var statsBlittable = await context.ParseToMemoryAsync(stream, "etl-stats", BlittableJsonDocumentBuilder.UsageMode.None, bytes);

                if (statsBlittable.TryGet("Results", out BlittableJsonReaderArray results))
                {
                    for (int resultIndex = 0; resultIndex < results.Length; resultIndex++)
                    {
                        var result = (BlittableJsonReaderObject)results[resultIndex];

                        if (result.TryGet("Stats", out BlittableJsonReaderArray statsArray))
                        {
                            for (int statsIndex = 0; statsIndex < statsArray.Length; statsIndex++)
                            {
                                var stat = (BlittableJsonReaderObject)statsArray[statsIndex];

                                if (stat.TryGet("Statistics", out BlittableJsonReaderObject statistics))
                                {
                                    // Parse LastAlert using AlertRaised.FromJson()
                                    if (statistics.TryGet("LastAlert", out BlittableJsonReaderObject lastAlert) &&
                                        lastAlert != null)
                                    {
                                        EtlErrorsDetails details = null;
                                        if (lastAlert.TryGet("Details", out BlittableJsonReaderObject detailsb) && detailsb != null)
                                        {
                                            details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter
                                                .FromBlittable<EtlErrorsDetails>(detailsb);
                                        }

                                        AlertRaised alertRaised = AlertRaised.FromJson("lastAlert", lastAlert, details);
                                        return alertRaised;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return null;
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
        private async Task CannotRetiredAttachmentsToOldInternal(S3Settings settings, DocumentStore store)
        {
            string identifier = await CreateRetiredAttachmentsConfigurationAndGetIdentifier(settings, store);

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
                    RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                    ContentType = "image/png"
                });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var exists = session.Advanced.Attachments.Exists(id, "test.png");
                Assert.True(exists);
            }

            await RetireAndAssertCount(store, settings, 1);

            using (var session = store.OpenSession())
            {
                var retiredExists = session.Advanced.Attachments.Exists(id, "test.png");
                Assert.True(retiredExists);
            }
        }

        private IAsyncDisposable DeleteObjects(S3Settings settings)
        {
            return new AsyncDisposableAction(async () =>
            {

                if (settings == null)
                    return;

                await S3Tests.DeleteObjects(settings, prefix: $"{settings.RemoteFolderName}", delimiter: string.Empty);
            });
        }

        private static async Task<string> CreateRetiredAttachmentsConfigurationAndGetIdentifier(S3Settings settings, DocumentStore store)
        {
            var identifier = "conf-identifier-s3";
            var config = new RetiredAttachmentsConfiguration()
            {
                Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                {
                    {
                        identifier, new RetiredAttachmentsDestinationConfiguration()
                        {
                            S3Settings = settings,
                            Disabled = false,
                            Identifier = identifier
                        }
                    }
                },
                RetireFrequencyInSec = 1000
            };

            await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(config));
            return identifier;
        }

        private async Task RetireAndAssertCount(DocumentStore store, S3Settings settings, int expected)
        {
            var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);


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

            var x123 = cloudObjects.Select(x => new RetiredAttachmentsHolderBase.FileInfoDetails()
            {
                FullPath = x.FullPath,
                LastModified = x.LastModified
            }).ToList();

            Assert.Equal(expected, x123.Count);
        }

    }
}
