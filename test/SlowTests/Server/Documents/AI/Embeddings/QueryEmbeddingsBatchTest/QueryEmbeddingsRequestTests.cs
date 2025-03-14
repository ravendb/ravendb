using System;
using System.Threading;
using FastTests;
using Raven.Server.Documents.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings.QueryEmbeddingsBatchTest;

public class QueryEmbeddingsRequestTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const string TestValue = "test text";

    [RavenFact(RavenTestCategory.Ai)]
    public void Creation_InitializesCorrectly()
    {
        // Arrange
        var callerToken = new CancellationTokenSource();

        // Act
        using var request = new QueryEmbeddingsRequest([TestValue], callerToken.Token);

        // Assert
        Assert.Equal(TestValue, request.Values[0]);
        Assert.False(request.TaskCompletionSource.Task.IsCompleted);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void CallerCancellation_CancelsTask()
    {
        // Arrange
        var callerToken = new CancellationTokenSource();

        using var request = new QueryEmbeddingsRequest([TestValue], callerToken.Token);

        // Act
        callerToken.Cancel();

        // Assert
        Assert.True(request.TaskCompletionSource.Task.IsCanceled, $"Expected task to be canceled, but it was '{request.TaskCompletionSource.Task.Status}'");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void CancelWithShutdownMessage_SetsTaskAsFaulted()
    {
        // Arrange
        using var request = new QueryEmbeddingsRequest([TestValue], CancellationToken.None);

        throw new NotImplementedException();

        //// Act
        //var task = request.CancelWithShutdownMessage();

        //// Assert
        //Assert.True(task.IsFaulted, $"Expected task to be faulted, but it was '{task.Status}'");
        //Assert.IsType<OperationCanceledException>(task.Exception.InnerException);
        //Assert.Contains(QueryEmbeddingsBatchingService.ShutdownMessage, task.Exception.InnerException.Message);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public void Dispose_ReleasesResources()
    {
        // Arrange
        var callerToken = new CancellationTokenSource();
        var request = new QueryEmbeddingsRequest([TestValue], callerToken.Token);

        // Act
        request.Dispose();

        // No assertion needed - we're just making sure no exceptions are thrown
    }
}
