using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ClusterTransactionTests : ClusterTransactionTestsBase
    {
        public ClusterTransactionTests(ITestOutputHelper output) : base(output)
        {
        }

        protected override RavenServer GetNewServer(ServerCreationOptions options = null, [CallerMemberName]string caller = null)
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

        protected override IDocumentStore InternalGetDocumentStore(Options options = null, string caller = null)
        {
            return GetDocumentStore(options, caller);
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task CanCreateClusterTransactionRequest()
        {
            await base.CanCreateClusterTransactionRequest();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task CanCreateClusterTransactionRequest2()
        {
            await base.CanCreateClusterTransactionRequest2();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ServeSeveralClusterTransactionRequests()
        {
            await base.ServeSeveralClusterTransactionRequests();
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        public override async Task CanPreformSeveralClusterTransactions(int numberOfNodes)
        {
            await base.CanPreformSeveralClusterTransactions(numberOfNodes);
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public override async Task ClusterTransactionWaitForIndexes(int docs)
        {
            await base.ClusterTransactionWaitForIndexes(docs);
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task CanImportExportAndBackupWithClusterTransactions()
        {
            await base.CanImportExportAndBackupWithClusterTransactions();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task TestSessionSequence()
        {
            await base.TestSessionSequence();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ResolveInFavorOfClusterTransaction()
        {
            await base.ResolveInFavorOfClusterTransaction();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task TestCleanUpClusterState()
        {
            await base.TestCleanUpClusterState();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task TestConcurrentClusterSessions()
        {
            await base.TestConcurrentClusterSessions();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task TestSessionMixture()
        {
            await base.TestSessionMixture();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task CreateUniqueUser()
        {
            await base.CreateUniqueUser();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task SessionCompareExchangeCommands()
        {
            await base.SessionCompareExchangeCommands();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterTxWithCounters()
        {
            await base.ClusterTxWithCounters();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override void ThrowOnClusterTransactionWithCounters()
        {
            base.ThrowOnClusterTransactionWithCounters();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override void ThrowOnClusterTransactionWithAttachments()
        {
            base.ThrowOnClusterTransactionWithAttachments();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override void ThrowOnClusterTransactionWithTimeSeries()
        {
            base.ThrowOnClusterTransactionWithTimeSeries();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ModifyDocumentWithRevision()
        {
            await base.ModifyDocumentWithRevision();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task PutDocumentInDifferentCollectionWithRevision()
        {
            await base.PutDocumentInDifferentCollectionWithRevision();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task PutDocumentInDifferentCollection()
        {
            await base.PutDocumentInDifferentCollection();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterTransactionRequestWithRevisions()
        {
            await base.ClusterTransactionRequestWithRevisions();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ThrowOnUnsupportedOperations()
        {
            await base.ThrowOnUnsupportedOperations();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ThrowOnOptimisticConcurrency()
        {
            await base.ThrowOnOptimisticConcurrency();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ThrowOnOptimisticConcurrencyForSingleDocument()
        {
            await base.ThrowOnOptimisticConcurrencyForSingleDocument();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ThrowOnInvalidTransactionMode()
        {
            await base.ThrowOnInvalidTransactionMode();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task CanAddNullValueToCompareExchange()
        {
            await base.CanAddNullValueToCompareExchange();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task CanGetListCompareExchange()
        {
            await base.CanGetListCompareExchange();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterTransactionConflict()
        {
            await base.ClusterTransactionConflict();
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(@"
")]
        public override async Task ClusterWideTransaction_WhenStoreDocWithEmptyStringId_ShouldThrowInformativeError(string id)
        {
            await base.ClusterWideTransaction_WhenStoreDocWithEmptyStringId_ShouldThrowInformativeError(id);
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterTransactionShouldBeRedirectedFromPromotableNode()
        {
            await base.ClusterTransactionShouldBeRedirectedFromPromotableNode();
        }
    }
}
