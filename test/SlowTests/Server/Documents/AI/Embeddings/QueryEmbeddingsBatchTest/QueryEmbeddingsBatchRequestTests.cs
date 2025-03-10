using System.Threading;
using FastTests;
using Raven.Server.Documents.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings.QueryEmbeddingsBatchTest;

public class QueryEmbeddingsBatchRequestTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const string TestValue = "test text";

    [RavenFact(RavenTestCategory.Ai)]
    public void Creation_InitializesCorrectly()
    {
        // Arrange
        var callerToken = new CancellationTokenSource();
        var workerToken = new CancellationTokenSource();

        // Act
        using var request = new QueryEmbeddingsBatchRequest([TestValue], callerToken.Token, workerToken.Token);

        // Assert
        Assert.Equal(TestValue, request.Values[0]);
        Assert.False(request.TaskCompletionSource.Task.IsCompleted);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void CallerCancellation_CancelsTask()
    {
        // Arrange
        var callerToken = new CancellationTokenSource();
        var workerToken = new CancellationTokenSource();

        using var request = new QueryEmbeddingsBatchRequest([TestValue], callerToken.Token, workerToken.Token);

        // Act
        callerToken.Cancel();

        // Assert
        Assert.True(request.TaskCompletionSource.Task.IsCanceled, $"Expected task to be canceled, but it was '{request.TaskCompletionSource.Task.Status}'");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void WorkerCancellation_CancelsTask()
    {
        // Arrange
        var callerToken = new CancellationTokenSource();
        var workerToken = new CancellationTokenSource();

        using var request = new QueryEmbeddingsBatchRequest([TestValue], callerToken.Token, workerToken.Token);

        // Act
        workerToken.Cancel();

        // Assert
        Assert.True(request.TaskCompletionSource.Task.IsCanceled, $"Expected task to be canceled, but it was '{request.TaskCompletionSource.Task.Status}'");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void Dispose_ReleasesResources()
    {
        // Arrange
        var callerToken = new CancellationTokenSource();
        var workerToken = new CancellationTokenSource();

        var request = new QueryEmbeddingsBatchRequest([TestValue], callerToken.Token, workerToken.Token);

        // Act
        request.Dispose();

        // No assertion needed - we're just making sure no exceptions are thrown
    }
}
