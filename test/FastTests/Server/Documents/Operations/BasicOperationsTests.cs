using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Threading;
using Xunit;
using Tests.Infrastructure;

namespace FastTests.Server.Documents.Operations
{
    public class BasicOperationsTests : RavenLowLevelTestBase
    {
        public BasicOperationsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Can_notify_about_operations_progress_and_completion()
        {
            using (var db = CreateDocumentDatabase())
            {
                var token = new OperationCancelToken(TimeSpan.FromMinutes(2), CancellationToken.None, CancellationToken.None);

                var notifications = new BlockingCollection<OperationStatusChange>();
                var mre = new ManualResetEventSlim(false);

                var operationId = db.Operations.GetNextOperationId();

                db.Changes.OnOperationStatusChange += notifications.Add;

                db.Operations.AddLocalOperation(
                    operationId,
                    0,
                    "Operations Test",
                    detailedDescription: null,
                    onProgress => Task.Factory.StartNew<IOperationResult>(() =>
                    {
                        var p = new DeterminateProgress
                        {
                            Total = 1024,
                            Processed = 0
                        };
                        onProgress(p);
                        mre.Wait(token.Token);
                        mre.Reset();

                        p.Processed = 500;
                        onProgress(p);

                        mre.Wait(token.Token);

                        return new SampleOperationResult
                        {
                            Message = "I'm done"
                        };
                    }),
                    token: token);

                OperationStatusChange change;
                Assert.True(notifications.TryTake(out change, TimeSpan.FromSeconds(1)));
                Assert.NotNull(change.OperationId);
                Assert.Equal(OperationStatus.InProgress, change.State.Status);
                Assert.Null(change.State.Result);
                var progress = change.State.Progress as DeterminateProgress;
                Assert.NotNull(progress);
                Assert.Equal(1024, progress.Total);
                Assert.Equal(0, progress.Processed);

                mre.Set();

                Assert.True(notifications.TryTake(out change, TimeSpan.FromSeconds(1)));
                Assert.NotNull(change.OperationId);
                Assert.Equal(OperationStatus.InProgress, change.State.Status);
                Assert.Null(change.State.Result);
                progress = change.State.Progress as DeterminateProgress;
                Assert.NotNull(progress);
                Assert.Equal(1024, progress.Total);
                Assert.Equal(500, progress.Processed);

                mre.Set();

                Assert.True(notifications.TryTake(out change, TimeSpan.FromSeconds(1)));
                Assert.NotNull(change.OperationId);
                Assert.Equal(OperationStatus.Completed, change.State.Status);
                Assert.NotNull(change.State.Result);
                Assert.Null(change.State.Progress);
                var result = change.State.Result as SampleOperationResult;
                Assert.NotNull(result);
                Assert.Equal("I'm done", result.Message);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Can_notify_about_exception_in_operation()
        {
            using (var db = CreateDocumentDatabase())
            {
                long operationId = db.Operations.GetNextOperationId();

                var notifications = new BlockingCollection<OperationStatusChange>();

                db.Changes.OnOperationStatusChange += notifications.Add;

                db.Operations.AddLocalOperation(
                    operationId,
                    0,
                    "Operations Test",
                    detailedDescription: null,
                    onProgress => Task.Factory.StartNew<IOperationResult>(() =>
                    {
                        throw new Exception("Something bad happened");
                    }),
                    token: OperationCancelToken.None);

                OperationStatusChange change;

                Assert.True(notifications.TryTake(out change, TimeSpan.FromSeconds(30)));
                Assert.NotNull(change.OperationId);
                Assert.Equal(OperationStatus.Faulted, change.State.Status);
                Assert.NotNull(change.State.Result);
                Assert.Null(change.State.Progress);
                var result = change.State.Result as OperationExceptionResult;
                Assert.NotNull(result);
                Assert.Equal("Something bad happened", result.Message);
                Assert.IsType<string>(result.Error);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Should_be_able_to_cancel_operation()
        {
            using (var db = CreateDocumentDatabase())
            {
                var token = new OperationCancelToken(TimeSpan.Zero, CancellationToken.None, CancellationToken.None);
                token.Cancel();

                var notifications = new BlockingCollection<OperationStatusChange>();

                var operationId = db.Operations.GetNextOperationId();

                db.Changes.OnOperationStatusChange += notifications.Add;

                db.Operations.AddLocalOperation(operationId,
                    0,
                    "Cancellation Test",
                    detailedDescription: null,
                    onProgress => Task.Factory.StartNew<IOperationResult>(() =>
                    {
                        token.Token.ThrowIfCancellationRequested();
                        return null;
                    }, token.Token), 
                    token: token);

                Assert.True(notifications.TryTake(out OperationStatusChange change, TimeSpan.FromSeconds(1)));
                Assert.NotNull(change.OperationId);
                Assert.Equal(OperationStatus.Canceled, change.State.Status);
                Assert.Null(change.State.Result);
                Assert.Null(change.State.Progress);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Can_serialize_in_progress_state_to_json()
        {
            var state = new OperationState
            {
                Progress = new DeterminateProgress
                {
                    Processed = 1,
                    Total = 100
                }
            };

            using (var context = new JsonOperationContext(1024, 1024, 32 * 1024, SharedMultipleUseFlag.None))
            {
                var json = context.ReadObject(state.ToJson(), "state");
                var progress = json["Progress"];
                Assert.NotNull(progress);
                Assert.Equal("InProgress", json["Status"].ToString());
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Can_serialize_completed_state_to_json()
        {
            var state = new OperationState
            {
                Status = OperationStatus.Completed,
                Result = new SampleOperationResult
                {
                    Message = "Done"
                }
            };

            using (var context = new JsonOperationContext(1024, 1024, 32 * 1024, SharedMultipleUseFlag.None))
            {
                var json = context.ReadObject(state.ToJson(), "state");
                var result = json["Result"] as BlittableJsonReaderObject;
                Assert.NotNull(result);
                Assert.Equal("Done", result["Message"].ToString());
            }
        }

        public enum Ending
        {
            Completed,
            Faulted,
            Canceled
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(Ending.Completed)]
        [InlineData(Ending.Faulted)]
        [InlineData(Ending.Canceled)]
        public void Finishing_an_operation_publishes_a_new_state_and_never_mutates_the_running_one(Ending ending)
        {
            using (var db = CreateDocumentDatabase())
            {
                var gate = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                var operationId = db.Operations.GetNextOperationId();

                db.Operations.AddLocalOperation(
                    operationId,
                    OperationType.DumpRawIndexData,
                    "Operations Test",
                    detailedDescription: null,
                    onProgress =>
                    {
                        onProgress(new DeterminateProgress
                        {
                            Total = 10,
                            Processed = 5
                        });

                        return gate.Task;
                    }).IgnoreUnobservedExceptions();

                var operation = db.Operations.GetOperation(operationId);
                Assert.True(SpinWait.SpinUntil(() => operation.State.Progress != null, TimeSpan.FromSeconds(15)));

                var running = operation.State;

                switch (ending)
                {
                    case Ending.Completed:
                        gate.SetResult(new SampleOperationResult { Message = "I'm done" });
                        break;
                    case Ending.Faulted:
                        gate.SetException(new InvalidOperationException("Something bad happened"));
                        break;
                    case Ending.Canceled:
                        gate.SetCanceled();
                        break;
                }

                Assert.True(SpinWait.SpinUntil(() => ReferenceEquals(operation.State, running) == false, TimeSpan.FromSeconds(15)));

                Assert.Equal(OperationStatus.InProgress, running.Status);
                Assert.Null(running.Result);
                Assert.NotNull(running.Progress);

                var finished = operation.State;

                switch (ending)
                {
                    case Ending.Completed:
                        Assert.Equal(OperationStatus.Completed, finished.Status);
                        Assert.Equal("I'm done", ((SampleOperationResult)finished.Result).Message);
                        break;
                    case Ending.Faulted:
                        Assert.Equal(OperationStatus.Faulted, finished.Status);
                        Assert.IsType<OperationExceptionResult>(finished.Result);
                        break;
                    case Ending.Canceled:
                        Assert.Equal(OperationStatus.Canceled, finished.Status);
                        Assert.Null(finished.Result);
                        break;
                }

                Assert.Null(finished.Progress);
            }
        }

        private class SampleOperationResult : IOperationResult
        {
            public string Message { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue(GetType())
                {
                    ["Message"] = Message
                };
            }

            public bool ShouldPersist => false;

            bool IOperationResult.CanMerge => false;

            void IOperationResult.MergeWith(IOperationResult result)
            {
                throw new NotImplementedException();
            }
        }
    }
}
