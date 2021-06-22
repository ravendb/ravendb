using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide.Operations.Certificates;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_13861 : RavenTestBase
    {
        public RavenDB_13861(ITestOutputHelper output) : base(output)
        {
        }

        [Fact32Bit]
        public async Task BatchMemorySizeLimitationShouldBeExactIn32Bit()
        {
            var str = string.Join(string.Empty, Enumerable.Range(0, 1600).Select(x => x.ToString()).ToArray());
            using (var store = GetDocumentStore())
            {
                using (var bi = store.BulkInsert())
                {
                    // with 747 documents, we pass the 4MB limit
                    for (var i = 0; i < 747 * 10; i++)
                    {
                        bi.Store(new Order
                        {
                            Company = str
                        });
                    }
                }

                var subsId = store.Subscriptions.Create(new Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions<Order>());
                var worker = store.Subscriptions.GetSubscriptionWorker<Order>(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
                {
                    CloseWhenNoDocsLeft = true
                });

                var batchLengths = new List<int>();
                await Assert.ThrowsAsync<SubscriptionClosedException>(async () => await worker.Run(batch =>
                {
                    batchLengths.Add(batch.Items.Count);
                }));
                Assert.All(batchLengths, x => Assert.Equal(747, x));
            }
        }

        [Fact32Bit]
        public async Task BatchMemorySizeLimitationShouldBeExactInEncryptedModeIn32Bit()
        {
            var str = string.Join(string.Empty, Enumerable.Range(0, 1600).Select(x => x.ToString()).ToArray());

            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }
            var base64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
#pragma warning disable CA1416 // Validate platform compatibility
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                RunInMemory = true
            }))
            {
                using (var bi = store.BulkInsert())
                {
                    // with 300 documents, we pass the 4MB limit with encrypted db
                    for (var i = 0; i < 300 * 10; i++)
                    {
                        bi.Store(new Order
                        {
                            Company = str
                        });
                    }
                }

                var subsId = store.Subscriptions.Create(new Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions<Order>());
                var worker = store.Subscriptions.GetSubscriptionWorker<Order>(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
                {
                    MaxDocsPerBatch = int.MaxValue,
                    CloseWhenNoDocsLeft = true
                });

                var batchLengths = new List<int>();
                await Assert.ThrowsAsync<SubscriptionClosedException>(async () => await worker.Run(batch =>
                {
                    batchLengths.Add(batch.Items.Count);
                }));
                Assert.All(batchLengths, x => Assert.True(Math.Abs(295 - x) < 10));
            }
        }

        [Fact64Bit]
        public async Task BatchMemorySizeLimitationShouldBeExactIn64Bit()
        {
            var str = string.Join(string.Empty, Enumerable.Range(0, 1600).Select(x => x.ToString()).ToArray());
            using (var store = GetDocumentStore())
            {
                using (var bi = store.BulkInsert())
                {
                    // with 747 documents, we pass the 32MB limit
                    for (var i = 0; i < 5972 * 3; i++)
                    {
                        bi.Store(new Order
                        {
                            Company = str
                        });
                    }
                }

                var subsId = store.Subscriptions.Create(new Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions<Order>());
                var worker = store.Subscriptions.GetSubscriptionWorker<Order>(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
                {
                    CloseWhenNoDocsLeft = true,
                    MaxDocsPerBatch = int.MaxValue
                });

                var batchLengths = new List<int>();
                await Assert.ThrowsAsync<SubscriptionClosedException>(async () => await worker.Run(batch =>
                {
                    batchLengths.Add(batch.Items.Count);
                }));
                Assert.All(batchLengths, x => Assert.Equal(5972, x));
            }
        }

        [Fact64Bit]
        public async Task BatchMemorySizeLimitationShouldBeExactInEncryptedModeIn64Bit()
        {
            var str = string.Join(string.Empty, Enumerable.Range(0, 1600).Select(x => x.ToString()).ToArray());

            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }
            var base64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
#pragma warning disable CA1416 // Validate platform compatibility
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                RunInMemory = true
                //Path = NewDataPath()
            }))
            {
                using (var bi = store.BulkInsert())
                {
                    // with 747 documents, we pass the 32MB limit
                    for (var i = 0; i < 2424 * 3; i++)
                    {
                        bi.Store(new Order
                        {
                            Company = str
                        });
                    }
                }

                var subsId = store.Subscriptions.Create(new Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions<Order>());
                var worker = store.Subscriptions.GetSubscriptionWorker<Order>(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
                {
                    MaxDocsPerBatch = int.MaxValue,
                    CloseWhenNoDocsLeft = true
                });

                var batchLengths = new List<int>();
                await Assert.ThrowsAsync<SubscriptionClosedException>(async () => await worker.Run(batch =>
                {
                    batchLengths.Add(batch.Items.Count);
                }));
                Assert.All(batchLengths, x => Assert.True(Math.Abs(2425 - x) < 10));
            }
        }
    }
}
