using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Attachments
{
    public class RetiredAttachmentsBasicTests : RavenTestBase
    {
        public RetiredAttachmentsBasicTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanPutAndRetiredAttachmentsConfigurationWithDefaultRetireFrequencyInSec()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "S3-Users", new RetiredAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new S3Settings()
                                {
                                    BucketName = "testS3Bucket-Users"
                                },
                                Disabled = false,
                                Identifier = "S3-Users"
                            }
                        }
                    },
                    MaxItemsToProcess = 1,
                }));

                var config = await store.Maintenance.SendAsync(new GetRetireAttachmentsConfigurationOperation());
                var destination = config.Destinations.FirstOrDefault();
                Assert.Equal(1, config.Destinations.Count);
                Assert.NotNull(destination);
                Assert.Equal("S3-Users", destination.Key);
                Assert.Equal("S3-Users", destination.Value.Identifier);
                Assert.Equal("testS3Bucket-Users", destination.Value.S3Settings.BucketName);
                Assert.Equal(false, destination.Value.Disabled);
                Assert.Equal(null, config.RetireFrequencyInSec);
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanPutAndGetRetiredAttachmentsConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var c1 = new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                       {
                           {
                               "S3-Users", new RetiredAttachmentsDestinationConfiguration()
                               {
                                   Identifier = "S3-Users",
                                   S3Settings = new S3Settings() { BucketName = "testS3Bucket-Users" },
                                   Disabled = false
                               }
                           }
                       },
                    RetireFrequencyInSec = 1000
                };

                await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(c1));

                var config = await store.Maintenance.SendAsync(new GetRetireAttachmentsConfigurationOperation());
                var destination = config.Destinations.FirstOrDefault();
                Assert.Equal(1, config.Destinations.Count);
                Assert.NotNull(destination);
                Assert.Equal("S3-Users", destination.Key);
                Assert.Equal("S3-Users", destination.Value.Identifier);
                Assert.Equal("testS3Bucket-Users", destination.Value.S3Settings.BucketName);
                Assert.Equal(false, destination.Value.Disabled);
                Assert.Equal(1000, config.RetireFrequencyInSec);

                var c2 = new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                           {
                               {
                                   "S3-Orders", new RetiredAttachmentsDestinationConfiguration()
                                   {
                                       Identifier = "S3-Orders",
                                       S3Settings = new S3Settings() { BucketName = "testS3Bucket-Orders" },
                                       Disabled = true,
                                   }
                               }
                           },
                    RetireFrequencyInSec = 10000
                };

                await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(c2));

                var config2 = await store.Maintenance.SendAsync(new GetRetireAttachmentsConfigurationOperation());
                var destination2 = config2.Destinations.FirstOrDefault();
                Assert.Equal(1, config2.Destinations.Count);
                Assert.NotNull(destination2);
                Assert.Equal("S3-Orders", destination2.Key);
                Assert.Equal("S3-Orders", destination2.Value.Identifier);
                Assert.Equal("testS3Bucket-Orders", destination2.Value.S3Settings.BucketName);
                Assert.Equal(true, destination2.Value.Disabled);
                Assert.Equal(10000, config2.RetireFrequencyInSec);
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanPutAndUpdateRetiredAttachmentsConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var c1 = new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "S3-Users", new RetiredAttachmentsDestinationConfiguration()
                            {
                                Identifier = "S3-Users",
                                S3Settings = new S3Settings() { BucketName = "testS3Bucket-Users" },
                                Disabled = false
                            }
                        }
                    },
                    RetireFrequencyInSec = 1000
                };

                await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(c1));

                var config = await store.Maintenance.SendAsync(new GetRetireAttachmentsConfigurationOperation());
                var destination = config.Destinations.FirstOrDefault();
                Assert.NotNull(destination);
                Assert.Equal("S3-Users", destination.Key);
                Assert.Equal("S3-Users", destination.Value.Identifier);
                Assert.Equal("testS3Bucket-Users", destination.Value.S3Settings.BucketName);
                Assert.Equal(false, destination.Value.Disabled);
                Assert.Equal(1000, config.RetireFrequencyInSec);
                config.Destinations.Add(
                    "S3-Orders",
                    new RetiredAttachmentsDestinationConfiguration()
                    {
                        Identifier = "S3-Orders", S3Settings = new S3Settings() { BucketName = "testS3Bucket-Orders" }, Disabled = true,
                    }
                );
                config.RetireFrequencyInSec = 10000;

                await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(config));

                var config2 = await store.Maintenance.SendAsync(new GetRetireAttachmentsConfigurationOperation());
                var destination2 = config2.Destinations.LastOrDefault();
                Assert.NotNull(destination2);
                Assert.Equal("S3-Orders", destination2.Key);
                Assert.Equal("S3-Orders", destination2.Value.Identifier);
                Assert.Equal("testS3Bucket-Orders", destination2.Value.S3Settings.BucketName);
                Assert.Equal(true, destination2.Value.Disabled);
                Assert.Equal(10000, config2.RetireFrequencyInSec);



                var config3 = await store.Maintenance.SendAsync(new GetRetireAttachmentsConfigurationOperation());

               config3.Destinations.TryGetValue("S3-Users", out var destUsers);
                Assert.NotNull(destUsers);
                Assert.Equal("S3-Users", destUsers.Identifier);
                Assert.Equal("testS3Bucket-Users", destUsers.S3Settings.BucketName);
                Assert.Equal(false, destUsers.Disabled);
                Assert.Equal(10000, config.RetireFrequencyInSec);

                config3.Destinations.TryGetValue("S3-Orders", out var destOrders);
                Assert.NotNull(destOrders);
                Assert.Equal("S3-Orders", destOrders.Identifier);
                Assert.Equal("testS3Bucket-Orders", destOrders.S3Settings.BucketName);
                Assert.Equal(true, destOrders.Disabled);
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanAssertRetiredAttachmentsConfiguration(bool disabled)
        {
            using (var store = GetDocumentStore())
            {
                Exception e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "S3-Users", new RetiredAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                                AzureSettings = new AzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
                                Disabled = disabled,
                            }
                        }
                    },
                    RetireFrequencyInSec = 1000,
                })));
                Assert.Contains("Identifier must have a value.", e.Message);
              
                e = await Assert.ThrowsAsync< ArgumentNullException> (async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            null, new RetiredAttachmentsDestinationConfiguration()
                            {
                                Identifier = null,
                                S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                                AzureSettings = new AzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
                                Disabled = disabled,
                            }
                        }
                    },
                    RetireFrequencyInSec = 1000,
                })));
                Assert.Contains("Value cannot be null. (Parameter 'key')", e.Message);

                e = await Assert.ThrowsAsync<ArgumentNullException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            null, new RetiredAttachmentsDestinationConfiguration()
                            {
                                Identifier = "S3-Users",
                                S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                                AzureSettings = new AzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
                                Disabled = disabled,
                            }
                        }
                    },
                    RetireFrequencyInSec = 1000,
                })));
                Assert.Contains("Value cannot be null. (Parameter 'key')", e.Message);

                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "S3-Users", new RetiredAttachmentsDestinationConfiguration()
                            {
                                Identifier = null,
                                S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                                AzureSettings = new AzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
                                Disabled = disabled,
                            }
                        }
                    },
                    RetireFrequencyInSec = 1000,
                })));
                Assert.Contains("Identifier must have a value.", e.Message);

                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RetiredAttachmentsDestinationConfiguration()
                            {
                                Identifier = "test",
                                S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                                AzureSettings = new AzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
                                Disabled = disabled,
                            }
                        }
                    },
                    RetireFrequencyInSec = 1000,
                })));
                Assert.Contains("Only one uploader for RetiredAttachmentsConfiguration can be configured", e.Message);
               
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RetiredAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new S3Settings()
                                {
                                    BucketName = "testS3Bucket"
                                },
                                Disabled = disabled,
                                Identifier = "test"                }}
                    },
                    RetireFrequencyInSec = 0,
                })));
                Assert.Contains("Retire attachments frequency must be greater than 0.", e.Message);
               
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RetiredAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new S3Settings()
                                {
                                    BucketName = "testS3Bucket"
                                },
                                Disabled = disabled,
                                Identifier = "test"
                            }
                        }
                    },
                    RetireFrequencyInSec = 1,
                    MaxItemsToProcess = 0,
                })));
                Assert.Contains("Max items to process must be greater than 0.", e.Message);
                
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()

                {

                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RetiredAttachmentsDestinationConfiguration()
                            {
                                Disabled = disabled,
                                Identifier = "test"
                            }
                        }
                    },
                    MaxItemsToProcess = 1,
                    RetireFrequencyInSec = 1,
                })));
                Assert.Contains("Exactly one uploader for RetiredAttachmentsConfiguration must be configured.", e.Message);
              
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RetiredAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new S3Settings()
                                {
                                    BucketName = "testS3Bucket"
                                },
                                AzureSettings = new AzureSettings()
                                {
                                    AccountName = "testAzureAccount", StorageContainer = "testAzureContainer"
                                },
                                Disabled = disabled,
                                Identifier = "test"
                            }
                        }
                    },
                    MaxItemsToProcess = 1,
                    RetireFrequencyInSec = 1,
                })));
                Assert.Contains("Only one uploader for RetiredAttachmentsConfiguration can be configured.", e.Message);

                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "test", new RetiredAttachmentsDestinationConfiguration()
                            {
                                Disabled = false,
                                S3Settings = new S3Settings()
                                {
                                    BucketName = "testS3Bucket"
                                },
                                Identifier = "conf-identifier",
                            }
                        },
                        {
                            "s3-conf-identifier", new RetiredAttachmentsDestinationConfiguration()
                            {
                                Disabled = false,
                                AzureSettings  = new AzureSettings()
                                {
                                    AccountName = "testAzureAccount", StorageContainer = "testAzureContainer"
                                },
                                Identifier = "s3-conf-identifier",
                            }
                        }
                    },
                    RetireFrequencyInSec = 1
                })));
                Assert.Contains("Identifier 'conf-identifier' does not match the key 'test'", e.Message);
            }
        }
    }
}
