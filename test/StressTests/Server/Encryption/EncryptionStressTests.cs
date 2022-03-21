using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Graph;
using Raven.Client.Documents;
using Raven.Server.Utils;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Server.Encryption
{
    public class EncryptionStressTests : ClusterTestBase
    {
        public EncryptionStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanLockMemoryUnderHighContention() // this is a probabilistic test
        {
            using var process = Process.GetCurrentProcess();
            IntPtr minWorkingSet = default;
            IntPtr maxWorkingSet = default;

            if (PlatformDetails.RunningOnPosix == false)
            {
                minWorkingSet = process.MinWorkingSet;
                maxWorkingSet = process.MaxWorkingSet;
            }

            try
            {
                DebuggerAttachedTimeout.DisableLongTimespan = true;
                var (nodes, leader, certificates) = await CreateRaftClusterWithSsl(7, watcherCluster: true);

                Encryption.EncryptedCluster(nodes, certificates, out var databaseName);

                var options = new Options
                {
                    Server = leader,
                    ReplicationFactor = 7,
                    ClientCertificate = certificates.ClientCertificate1.Value,
                    AdminCertificate = certificates.ServerCertificate.Value,
                    ModifyDatabaseName = _ => databaseName,
                    Encrypted = true,
                    RunInMemory = false
                };
                using (var store = GetDocumentStore(options))
                {
                    await TrySavingDocument(store, 6);
                }
            }
            finally
            {
                if (PlatformDetails.RunningOnPosix == false)
                {
#pragma warning disable CA1416 // Validate platform compatibility
                    process.MinWorkingSet = minWorkingSet;
                    process.MaxWorkingSet = maxWorkingSet;
#pragma warning restore CA1416 // Validate platform compatibility
                }
            }
            
        }

        private static async Task TrySavingDocument(DocumentStore store, int? replicas = null)
        {
            using (var session = store.OpenAsyncSession())
            {
                if (replicas.HasValue)
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: replicas.Value);

                await session.StoreAsync(new User { Name = "Foo" });
                await session.SaveChangesAsync();
            }
        }
    }
}
