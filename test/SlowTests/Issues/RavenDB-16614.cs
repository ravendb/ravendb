using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Server;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16614 : ReplicationTestBase
    {
        public RavenDB_16614(ITestOutputHelper output) : base(output)
        {
        }

        protected override RavenServer GetNewServer(ServerCreationOptions options = null, [CallerMemberName] string caller = null)
        {
            if (options == null)
            {
                options = new ServerCreationOptions();
            }

            if (options.CustomSettings == null)
                options.CustomSettings = new Dictionary<string, string>();

            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "60";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "10";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.TcpConnectionTimeout)] = "30000";

            return base.GetNewServer(options, caller);
        }
        private class User
        {
            public string Name;
        }

        [Fact]
        public async Task WillMarkClusterWideDocumentsWithTransactionId()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            using var store = GetDocumentStore(new Options {Server = leader, ReplicationFactor = 3});

            using (var session = store.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
            {
                await session.StoreAsync(new User {Name = "arava"}, "users/arava");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var arava = await session.LoadAsync<User>("users/arava");
                var metadata = session.Advanced.GetMetadataFor(arava);
                var cti = (IMetadataDictionary)metadata[Constants.Documents.Metadata.ClusterTransactionIndex];
                var txid = cti[cti.Keys.Single()];
                var guard = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<object>("rvn-atomic-guard/users/arava");
                Assert.Equal(txid, guard.Index);
            }
        }

        [Fact]
        public async Task WillGetGoodErrorOnMismatchClusterTxId()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var arava = await session.LoadAsync<User>("users/arava");
                var metadata = session.Advanced.GetMetadataFor(arava);
                var cti = (IMetadataDictionary)metadata[Constants.Documents.Metadata.ClusterTransactionIndex];
                var key = cti.Keys.Single();
                cti[key] = (long)(cti[key]) + 2;
                arava.Name += "-modified";
                var err = await Assert.ThrowsAsync<ConcurrencyException>(() => session.SaveChangesAsync());
                Assert.Contains("Failed to execute cluster transaction due to the following issues: " +
                    "Guard compare exchange value 'rvn-atomic-guard/users/arava' index does not match " +
                    "'@metadata'.'@cluster-transaction-index'", err.Message);
            }
        }

        [Fact]
        public async Task WillFailNormalTransactionThatDoesNotMatchAtomicGuardIndex()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { 
                TransactionMode = TransactionMode.SingleNode // important, NOT a cluster wide transaction
            }))
            {
                var arava = await session.LoadAsync<User>("users/arava");
                var metadata = session.Advanced.GetMetadataFor(arava);
                var cti = (IMetadataDictionary)metadata[Constants.Documents.Metadata.ClusterTransactionIndex];
                var key = cti.Keys.Single();
                cti[key] = (long)(cti[key]) + 2;
                arava.Name += "-modified";
                
                var err = await Assert.ThrowsAsync<ConcurrencyException>(() => session.SaveChangesAsync());
                Assert.Contains("Cannot PUT document 'users/arava' because its '@metadata'.'@cluster-transaction-index'", err.Message);
                Assert.Contains("but the compare exchange guard ('rvn-atomic-guard/users/arava') is set to", err.Message);
            }
        }

        [Fact]
        public async Task CanDeleteCmpXchgValue()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.SaveChangesAsync();
            }
            WaitForUserToContinueTheTest(store);
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                session.Delete("users/arava");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var arava = await session.LoadAsync<User>("users/arava");
                Assert.Null(arava);
                var guard = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<object>("rvn-atomic-guard/users/arava");
                Assert.Null(guard);
            }
        }


        [Fact]
        public async Task CanModifyDocumentAfterFirstTime()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.StoreAsync(new User { Name = "phoebe" }, "users/phoebe");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var arava = await session.LoadAsync<User>("users/arava");
                arava.Name = "Arava Eini";
                var phoebe = await session.LoadAsync<User>("users/phoebe");
                phoebe.Name = "Phoebe Eini";
                await session.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task ModificationInAnotherTransactionWillFail()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.StoreAsync(new User { Name = "phoebe" }, "users/phoebe");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var user = await session.LoadAsync<User>("users/arava");
                user.Name = "Arava Eini";
                var user2 = await session.LoadAsync<User>("users/phoebe");
                user2.Name = "Phoebe Eini";

                using (var conflictedSession = store.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
                {
                    var conflictedArava = await conflictedSession.LoadAsync<User>("users/arava");
                    conflictedArava.Name = "Arava!";
                    await conflictedSession.SaveChangesAsync();
                }

                await Assert.ThrowsAsync<ConcurrencyException>(() => session.SaveChangesAsync());
            }
        }


        [Fact]
        public async Task ModificationInAnotherTransactionWillFailWithDelete()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.StoreAsync(new User { Name = "phoebe" }, "users/phoebe");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var user = await session.LoadAsync<User>("users/arava");
                session.Delete(user);
                var user2 = await session.LoadAsync<User>("users/phoebe");
                user2.Name = "Phoebe Eini";

                using (var conflictedSession = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var conflictedArava = await conflictedSession.LoadAsync<User>("users/arava");
                    conflictedArava.Name = "Arava!";
                    await conflictedSession.SaveChangesAsync();
                }

                await Assert.ThrowsAsync<ConcurrencyException>(() => session.SaveChangesAsync());
            }
        }
    }
}
