using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class NServiceBusCaseTest : ClusterTestBase
    {
        public NServiceBusCaseTest(ITestOutputHelper output) : base(output)
        {
        }

        const string SagaDataIdPrefix = "SampleSagaDatas";
        const int NumberOfConcurrentUpdates = 50;
        const int MaxRetryAttempts = 50;

        [Fact]
        public async Task ConcurrentArrayUpdate()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                {
                    ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin, 
                    LoadBalanceBehavior = LoadBalanceBehavior.UseSessionContext
                },
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {

                var sagaDataStableId = SagaDataIdPrefix + "/" + Guid.NewGuid();
                var results = await Execute(store, sagaDataStableId);

                await ValidateResults(store, sagaDataStableId, results);

                var failedUpdates = results.Where(r => !r.Succeeded).ToList();

                if (failedUpdates.Any())
                {
                    Assert.True(false, $"{failedUpdates.Count} updated failed. {string.Join(Environment.NewLine, failedUpdates.Select(f => f.ErrorMessage))}");
                }

                using var checkSession = store.OpenAsyncSession();
                var sagaData = await checkSession.LoadAsync<SampleSagaData>(sagaDataStableId);

                var diff = Enumerable.Range(0, NumberOfConcurrentUpdates - 1)
                    .Except(failedUpdates.Select(fu => fu.Index))
                    .Except(sagaData.HandledIndexes)
                    .ToList();

                if (diff.Any())
                {
                    Assert.True(false, $"Cannot find an update for the following index(es): {string.Join(Environment.NewLine, diff)}");
                }
            }
        }

        private static async Task<bool> ValidateResults(IDocumentStore store, string sagaDataStableId, (bool Succeeded, int Index, string ErrorMessage)[] results)
        {
            using var checkSession = store.OpenAsyncSession();
            var sagaData = await checkSession.LoadAsync<SampleSagaData>(sagaDataStableId);

            var updatesFailedDueToConcurrency = results.Where(r => !r.Succeeded).ToList();

            var diff = Enumerable.Range(0, NumberOfConcurrentUpdates - 1)
                .Except(updatesFailedDueToConcurrency.Select(fu => fu.Index))
                .Except(sagaData.HandledIndexes)
                .ToList();

            return diff.Count == 0;
        }

        static async Task<(bool Succeeded, int Index, string ErrorMessage)[]> Execute(IDocumentStore store, string sagaDataStableId)
        {

            using (var storeItOnceSession = store.OpenAsyncSession(new SessionOptions() {TransactionMode = TransactionMode.ClusterWide}))
            {
                await storeItOnceSession.StoreAsync(new SampleSagaData() {Id = sagaDataStableId});
                await storeItOnceSession.SaveChangesAsync();
            }

            var pendingTasks = new List<Task<(bool Succeeded, int Index, string ErrorMessage)>>();
            for (var i = 0; i < NumberOfConcurrentUpdates; i++)
            {
                pendingTasks.Add(TouchSaga(i, store, sagaDataStableId));
            }

            return await Task.WhenAll(pendingTasks);
        }

        static async Task<(bool, int, string)> TouchSaga(int index, IDocumentStore store, string sagaDataStableId)
        {
            var attempts = 0;
            Exception lastError = null;

            while (attempts <= MaxRetryAttempts)
            {
                try
                {
                    using var session = store.OpenAsyncSession(new SessionOptions() {TransactionMode = TransactionMode.ClusterWide});
                    session.Advanced.SessionInfo.SetContext(index.ToString()); // distribute the writes
                    var sagaData = await session.LoadAsync<SampleSagaData>(sagaDataStableId);
                    sagaData.HandledIndexes.Add(index);
                    await session.SaveChangesAsync();

                    return (true, index, string.Empty);
                }
                catch (Exception ex)
                {
                    attempts++;
                    lastError = ex;
                    await Task.Delay(50);
                }
            }

            return (false, index, $"Failed after {attempts} attempts, while handling index {index}, last error: {lastError?.Message}");
        }
    }

    class SampleSagaData
    {
        public string Id { get; set; }
        public List<int> HandledIndexes { get; set; } = new List<int>();
    }
}
