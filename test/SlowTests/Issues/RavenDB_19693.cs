using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Operation = Raven.Client.Documents.Operations.Operation;

namespace SlowTests.Issues;

public class RavenDB_19693 : RavenTestBase
{
    public RavenDB_19693(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Can_Use_CancellationToken_In_WaitForCompletion()
    {
        using (var store = GetDocumentStore())
        {
            var database = await GetDatabase(store.Database);
            var operationId = database.Operations.GetNextOperationId();
            var token = new OperationCancelToken(database.DatabaseShutdown, CancellationToken.None);
            _ = database.Operations.AddLocalOperation(operationId, OperationType.DumpRawIndexData, "Test Operation", detailedDescription: null, onProgress => DoWorkAsync(onProgress, TimeSpan.FromSeconds(2), token.Token), token: token);

            var operation = new Operation(store.GetRequestExecutor(), () => store.Changes(store.Database, Server.ServerStore.NodeTag), store.Conventions, operationId, Server.ServerStore.NodeTag);

            using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(10)))
                await Assert.ThrowsAsync<TimeoutException>(() => operation.WaitForCompletionAsync(cts.Token));

            using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(10)))
                Assert.Throws<TimeoutException>(() => operation.WaitForCompletion(cts.Token));
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Can_Use_Kill()
    {
        using (var store = GetDocumentStore())
        {
            var database = await GetDatabase(store.Database);
            var operationId = database.Operations.GetNextOperationId();
            var token = new OperationCancelToken(database.DatabaseShutdown, CancellationToken.None);
            _ = database.Operations.AddLocalOperation(operationId, OperationType.DumpRawIndexData, "Test Operation", detailedDescription: null, onProgress => DoWorkAsync(onProgress, TimeSpan.FromSeconds(5), token.Token), token: token);

            var operation = new Operation(store.GetRequestExecutor(), () => store.Changes(store.Database, Server.ServerStore.NodeTag), store.Conventions, operationId, Server.ServerStore.NodeTag);

            await operation.KillAsync();

            await Assert.ThrowsAsync<TaskCanceledException>(() => operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30)));
        }
    }

    private static async Task<IOperationResult> DoWorkAsync(Action<IOperationProgress> onProgress, TimeSpan timeout, CancellationToken token)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            token.ThrowIfCancellationRequested();

            await Task.Delay(100);
        }

        return new AdminIndexHandler.DumpIndexResult();
    }
}
