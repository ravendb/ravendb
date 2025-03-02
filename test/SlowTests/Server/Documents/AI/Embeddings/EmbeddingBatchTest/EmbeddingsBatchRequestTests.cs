using System.Threading;
using Raven.Server.Documents.AI.Embeddings;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings.EmbeddingBatchTest;

public class EmbeddingsBatchRequestTests
{
    [Fact]
    public void Creation_InitializesCorrectly()
    {
        // Arrange
        const string value = "test text";
        var callerToken = new CancellationTokenSource();
        var workerToken = new CancellationTokenSource();

        // Act
        using var request = new EmbeddingsBatchRequest(value, callerToken.Token, workerToken.Token);

        // Assert
        Assert.Equal(value, request.Value);
        Assert.False(request.TaskCompletionSource.Task.IsCompleted);
    }

    [Fact]
    public void CallerCancellation_CancelsTask()
    {
        // Arrange
        var callerToken = new CancellationTokenSource();
        var workerToken = new CancellationTokenSource();

        using var request = new EmbeddingsBatchRequest("test", callerToken.Token, workerToken.Token);

        // Act
        callerToken.Cancel();

        // Assert
        Assert.True(request.TaskCompletionSource.Task.IsCanceled, $"Expected task to be canceled, but it was '{request.TaskCompletionSource.Task.Status}'");
    }

    [Fact]
    public void WorkerCancellation_CancelsTask()
    {
        // Arrange
        var callerToken = new CancellationTokenSource();
        var workerToken = new CancellationTokenSource();

        using var request = new EmbeddingsBatchRequest("test", callerToken.Token, workerToken.Token);

        // Act
        workerToken.Cancel();

        // Assert
        Assert.True(request.TaskCompletionSource.Task.IsCanceled, $"Expected task to be canceled, but it was '{request.TaskCompletionSource.Task.Status}'");
    }

    [Fact]
    public void Dispose_ReleasesResources()
    {
        // Arrange
        var callerToken = new CancellationTokenSource();
        var workerToken = new CancellationTokenSource();

        var request = new EmbeddingsBatchRequest("test", callerToken.Token, workerToken.Token);

        // Act
        request.Dispose();

        // No assertion needed - we're just making sure no exceptions are thrown
    }
}
