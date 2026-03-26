using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments.Remote;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Attachments
{
    public class RemoteAttachmentsBasicTests : RavenTestBase
    {
        public RemoteAttachmentsBasicTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments, LicenseRequired = true)]
        public async Task CanPutAndGetRemoteAttachmentsConfigurationWithCaseInsensitiveIdentifier()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "S3-uSeRs", new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings()
                                {
                                    BucketName = "testS3Bucket-Users"
                                },
                                Disabled = false,
                            }
                        }
                    },
                    MaxItemsToProcess = 1,
                }));

                var config = await store.Maintenance.SendAsync(new GetRemoteAttachmentsConfigurationOperation());
                var destination = config.Destinations.FirstOrDefault();
                Assert.Equal(1, config.Destinations.Count);
                Assert.NotNull(destination);
                Assert.Equal("S3-uSeRs", destination.Key);
                Assert.Equal("testS3Bucket-Users", destination.Value.S3Settings.BucketName);
                Assert.Equal(false, destination.Value.Disabled);
                Assert.Equal(null, config.CheckFrequencyInSec);
            }
        }

        [RavenFact(RavenTestCategory.Attachments, LicenseRequired = true)]
        public async Task CanPutAndGetRemoteAttachmentsConfigurationWithDefaultRemoteFrequencyInSec()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "S3-Users", new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings()
                                {
                                    BucketName = "testS3Bucket-Users"
                                },
                                Disabled = false,
                            }
                        }
                    },
                    MaxItemsToProcess = 1,
                }));

                var config = await store.Maintenance.SendAsync(new GetRemoteAttachmentsConfigurationOperation());
                var destination = config.Destinations.FirstOrDefault();
                Assert.Equal(1, config.Destinations.Count);
                Assert.NotNull(destination);
                Assert.Equal("S3-Users", destination.Key);
                Assert.Equal("testS3Bucket-Users", destination.Value.S3Settings.BucketName);
                Assert.Equal(false, destination.Value.Disabled);
                Assert.Equal(null, config.CheckFrequencyInSec);
            }
        }

        [RavenFact(RavenTestCategory.Attachments, LicenseRequired = true)]
        public async Task CanPutAndGetRemoteAttachmentsConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var c1 = new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                       {
                           {
                               "S3-Users", new RemoteAttachmentsDestinationConfiguration()
                               {
                                   S3Settings = new RemoteAttachmentsS3Settings() { BucketName = "testS3Bucket-Users" },
                                   Disabled = false
                               }
                           }
                       },
                    CheckFrequencyInSec = 1000
                };

                await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(c1));

                var config = await store.Maintenance.SendAsync(new GetRemoteAttachmentsConfigurationOperation());
                var destination = config.Destinations.FirstOrDefault();
                Assert.Equal(1, config.Destinations.Count);
                Assert.NotNull(destination);
                Assert.Equal("S3-Users", destination.Key);
                Assert.Equal("testS3Bucket-Users", destination.Value.S3Settings.BucketName);
                Assert.Equal(false, destination.Value.Disabled);
                Assert.Equal(1000, config.CheckFrequencyInSec);

                var c2 = new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                           {
                               {
                                   "S3-Orders", new RemoteAttachmentsDestinationConfiguration()
                                   {
                                       S3Settings = new RemoteAttachmentsS3Settings() { BucketName = "testS3Bucket-Orders" },
                                       Disabled = true,
                                   }
                               }
                           },
                    CheckFrequencyInSec = 10000,
                    Disabled = true,
                };

                await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(c2));

                var config2 = await store.Maintenance.SendAsync(new GetRemoteAttachmentsConfigurationOperation());
                var destination2 = config2.Destinations.FirstOrDefault();
                Assert.Equal(1, config2.Destinations.Count);
                Assert.Equal(true, config2.Disabled);
                Assert.NotNull(destination2);
                Assert.Equal("S3-Orders", destination2.Key);
                Assert.Equal("testS3Bucket-Orders", destination2.Value.S3Settings.BucketName);
                Assert.Equal(true, destination2.Value.Disabled);
                Assert.Equal(10000, config2.CheckFrequencyInSec);
            }
        }

        [RavenFact(RavenTestCategory.Attachments, LicenseRequired = true)]
        public async Task CanPutAndUpdateRemoteAttachmentsConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var c1 = new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "S3-Users", new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings() { BucketName = "testS3Bucket-Users" },
                                Disabled = false
                            }
                        }
                    },
                    CheckFrequencyInSec = 1000
                };

                await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(c1));

                var config = await store.Maintenance.SendAsync(new GetRemoteAttachmentsConfigurationOperation());
                var destination = config.Destinations.FirstOrDefault();
                Assert.NotNull(destination);
                Assert.Equal("S3-Users", destination.Key);
                Assert.Equal("testS3Bucket-Users", destination.Value.S3Settings.BucketName);
                Assert.Equal(false, destination.Value.Disabled);
                Assert.Equal(1000, config.CheckFrequencyInSec);
                config.Destinations.Add(
                    "S3-Orders",
                    new RemoteAttachmentsDestinationConfiguration()
                    {
                        S3Settings = new RemoteAttachmentsS3Settings() { BucketName = "testS3Bucket-Orders" },
                        Disabled = true,
                    }
                );
                config.CheckFrequencyInSec = 10000;

                await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(config));

                var config2 = await store.Maintenance.SendAsync(new GetRemoteAttachmentsConfigurationOperation());
                var destination2 = config2.Destinations.LastOrDefault();
                Assert.NotNull(destination2);
                Assert.Equal("S3-Orders", destination2.Key);
                Assert.Equal("testS3Bucket-Orders", destination2.Value.S3Settings.BucketName);
                Assert.Equal(true, destination2.Value.Disabled);
                Assert.Equal(10000, config2.CheckFrequencyInSec);



                var config3 = await store.Maintenance.SendAsync(new GetRemoteAttachmentsConfigurationOperation());

                config3.Destinations.TryGetValue("S3-Users", out var destUsers);
                Assert.NotNull(destUsers);
                Assert.Equal("testS3Bucket-Users", destUsers.S3Settings.BucketName);
                Assert.Equal(false, destUsers.Disabled);
                Assert.Equal(10000, config.CheckFrequencyInSec);

                config3.Destinations.TryGetValue("S3-Orders", out var destOrders);
                Assert.NotNull(destOrders);
                Assert.Equal("testS3Bucket-Orders", destOrders.S3Settings.BucketName);
                Assert.Equal(true, destOrders.Disabled);
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanAssertRemoteAttachmentsConfiguration(bool disabled)
        {
            using (var store = GetDocumentStore())
            {
                Exception e = await Assert.ThrowsAsync<ArgumentNullException>(async () => await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            null, new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings() { BucketName = "testS3Bucket" },
                                AzureSettings = new RemoteAttachmentsAzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
                                Disabled = disabled,
                            }
                        }
                    },
                    CheckFrequencyInSec = 1000,
                })));
                Assert.Contains("Value cannot be null. (Parameter 'key')", e.Message);

                e = await Assert.ThrowsAsync<ArgumentNullException>(async () => await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            null, new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings() { BucketName = "testS3Bucket" },
                                AzureSettings = new RemoteAttachmentsAzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
                                Disabled = disabled,
                            }
                        }
                    },
                    CheckFrequencyInSec = 1000,
                })));
                Assert.Contains("Value cannot be null. (Parameter 'key')", e.Message);

                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings() { BucketName = "testS3Bucket" },
                                AzureSettings = new RemoteAttachmentsAzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
                                Disabled = disabled,
                            }
                        }
                    },
                    CheckFrequencyInSec = 1000,
                })));
                Assert.Contains("Only one uploader for RemoteAttachmentsConfiguration can be configured", e.Message);

                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings()
                                {
                                    BucketName = "testS3Bucket"
                                },
                                Disabled = disabled,
                            }
                        }
                    },
                    CheckFrequencyInSec = 0,
                })));
                Assert.Contains("Remote attachments check frequency must be greater than 0.", e.Message);

                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings()
                                {
                                    BucketName = "testS3Bucket"
                                },
                                Disabled = disabled,
                            }
                        }
                    },
                    CheckFrequencyInSec = 1,
                    MaxItemsToProcess = 0,
                })));
                Assert.Contains("Max items to process must be greater than 0.", e.Message);

                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()

                {

                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RemoteAttachmentsDestinationConfiguration()
                            {
                                Disabled = disabled,
                            }
                        }
                    },
                    MaxItemsToProcess = 1,
                    CheckFrequencyInSec = 1,
                })));
                Assert.Contains("Exactly one uploader for RemoteAttachmentsConfiguration must be configured.", e.Message);

                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings()
                                {
                                    BucketName = "testS3Bucket"
                                },
                                AzureSettings = new RemoteAttachmentsAzureSettings()
                                {
                                    AccountName = "testAzureAccount", StorageContainer = "testAzureContainer"
                                },
                                Disabled = disabled,
                            }
                        }
                    },
                    MaxItemsToProcess = 1,
                    CheckFrequencyInSec = 1,
                })));
                Assert.Contains("Only one uploader for RemoteAttachmentsConfiguration can be configured.", e.Message);

                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "S3-Users", null
                        }
                    },
                    CheckFrequencyInSec = 1000,
                })));
                Assert.Contains("Destination configuration for key S3-Users is null", e.Message);

                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RemoteAttachmentsDestinationConfiguration()
                            {
                                Disabled = false,
                                S3Settings = new RemoteAttachmentsS3Settings()
                                {
                                    BucketName = "testS3Bucket"
                                },
                            }
                        },
                        {
                            "TEST", new RemoteAttachmentsDestinationConfiguration()
                            {
                                Disabled = false,
                                AzureSettings  = new RemoteAttachmentsAzureSettings()
                                {
                                    AccountName = "testAzureAccount", StorageContainer = "testAzureContainer"
                                },
                            }
                        }
                    },
                    CheckFrequencyInSec = 1
                })));
                Assert.Contains($"Destination key 'TEST' is duplicate. Duplicate keys are not allowed in remote attachments configuration", e.Message);

            }
        }

        [RavenFact(RavenTestCategory.Attachments | RavenTestCategory.Indexes)]
        public void RemoteAttachmentIndexWithFlags_ShouldCompile()
        {
            using (var store = GetDocumentStore())
            {
                var index = new RemoteAttachmentIndexWithFlags
                {
                    Conventions = store.Conventions
                };

                var indexDefinition = index.CreateIndexDefinition();

                Assert.NotNull(indexDefinition);
                Assert.Equal("RemoteAttachmentIndexWithFlags", indexDefinition.Name);
                Assert.NotEmpty(indexDefinition.Maps);

                var map = indexDefinition.Maps.First();

                var occurrences1 = map.Split(["this0.att.RemoteFlags.ToString() == Raven.Client.Documents.Attachments.RemoteAttachmentFlags.None.ToString()"], StringSplitOptions.None).Length - 1;
                Assert.Equal(2, occurrences1);

                var occurrences2 = map.Split(["this1.this0.att.RemoteFlags == Raven.Client.Documents.Attachments.RemoteAttachmentFlags.None"], StringSplitOptions.None).Length - 1;
                Assert.Equal(2, occurrences2);

                // Verify index can be executed/compiled without errors
                index.Execute(store);

                // Wait for indexing to ensure it compiles on the server side
                Indexes.WaitForIndexing(store);

                // Verify no compilation errors
                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        [RavenFact(RavenTestCategory.Attachments | RavenTestCategory.Sharding, LicenseRequired = true)]
        public async Task ShouldSkipRemoteAttachmentsConfigOnImportToShardedDatabase()
        {
            using (var store = GetDocumentStore())
            using (var sharded = Sharding.GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "S3-uSeRs", new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings()
                                {
                                    BucketName = "testS3Bucket-Users"
                                },
                                Disabled = false,
                            }
                        }
                    },
                    MaxItemsToProcess = 1,
                }));

                using (var dest = new MemoryStream())
                {
                    var export = await store.Smuggler.ExportToStreamAsync(new DatabaseSmugglerExportOptions(), s => s.CopyToAsync(dest));
                    await export.WaitForCompletionAsync();
                    dest.Position = 0;
                    var import = await sharded.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                    {
                    }, dest);
                    await import.WaitForCompletionAsync();
                }

                var shardedRecord = await sharded.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(sharded.Database));
                Assert.Null(shardedRecord.RemoteAttachments);
            }
        }

        internal class User
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string AttName { get; set; }
        }

        private class RemoteAttachmentIndexWithFlags : AbstractIndexCreationTask<User, RemoteAttachmentIndexWithFlags.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public RemoteAttachmentFlags Remote { get; set; }
                public DateTime? RemoteAt { get; set; }
                public string AttStr { get; set; }
                public string AttStr2 { get; set; }
                public string AttEnum { get; set; }
                public string AttEnum2 { get; set; }
            }

            public RemoteAttachmentIndexWithFlags()
            {
                Map = users => from u in users
                    let att = LoadAttachment(u, u.AttName)
                    let isLocalStr = att.RemoteFlags.ToString() == RemoteAttachmentFlags.None.ToString()
                    let isLocal = att.RemoteFlags == RemoteAttachmentFlags.None
                    select new Result
                    {
                        Name = att.Name,
                        Remote = att.RemoteFlags,
                        RemoteAt = att.RemoteAt,
                        AttStr = isLocalStr ? att.GetContentAsString() : "remote",
                        AttStr2 = att.RemoteFlags.ToString() == RemoteAttachmentFlags.None.ToString() ? att.GetContentAsString() : "remote",
                        AttEnum = isLocal ? att.GetContentAsString() : "remote",
                        AttEnum2 = att.RemoteFlags == RemoteAttachmentFlags.None ? att.GetContentAsString() : "remote",

                    };
                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
