using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Threading;
using Xunit;

namespace FastTests.Server.Documents.Operations
{
    public class BasicOperationsTests : RavenLowLevelTestBase
    {
        [Fact]
        public void Can_notify_about_operations_progress_and_completion()
        {
            using (var db = CreateDocumentDatabase())
            {
                var token = new OperationCancelToken(TimeSpan.FromMinutes(2), CancellationToken.None);

                var notifications = new BlockingCollection<OperationStatusChange>();
                var mre = new ManualResetEventSlim(false);

                var operationId = db.Operations.GetNextOperationId();

                db.Changes.OnOperationStatusChange += notifications.Add;

                db.Operations.AddOperation(null,"Operations Test", (Raven.Server.Documents.Operations.Operations.OperationType) 0, 
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
                    }), operationId, token: token);

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

        [Fact]
        public void Can_notify_about_exception_in_operation()
        {
            using (var db = CreateDocumentDatabase())
            {
                long operationId = db.Operations.GetNextOperationId();

                var notifications = new BlockingCollection<OperationStatusChange>();

                db.Changes.OnOperationStatusChange += notifications.Add;

                db.Operations.AddOperation(null, "Operations Test", (Raven.Server.Documents.Operations.Operations.OperationType)0,
                    onProgress => Task.Factory.StartNew<IOperationResult>(() =>
                    {
                       throw new Exception("Something bad happened");
                    }), operationId, token: OperationCancelToken.None);

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

        [Fact]
        public void Should_be_able_to_cancel_operation()
        {
            using (var db = CreateDocumentDatabase())
            {
                var token = new OperationCancelToken(TimeSpan.Zero, CancellationToken.None);
                token.Cancel();

                var notifications = new BlockingCollection<OperationStatusChange>();

                var operationId = db.Operations.GetNextOperationId();

                db.Changes.OnOperationStatusChange += notifications.Add;

                db.Operations.AddOperation(null, "Cancellation Test", (Raven.Server.Documents.Operations.Operations.OperationType)0,
                    onProgress => Task.Factory.StartNew<IOperationResult>(() =>
                    {
                        token.Token.ThrowIfCancellationRequested();
                        return null;
                    }, token.Token), operationId, token: token);

                OperationStatusChange change;

                Assert.True(notifications.TryTake(out change, TimeSpan.FromSeconds(1)));
                Assert.NotNull(change.OperationId);
                Assert.Equal(OperationStatus.Canceled, change.State.Status);
                Assert.Null(change.State.Result);
                Assert.Null(change.State.Progress);
            }
        }

        [Fact]
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

            using (var context = new JsonOperationContext(1024, 1024, SharedMultipleUseFlag.None))
            {
                var json = context.ReadObject(state.ToJson(), "state");
                var progress = json["Progress"];
                Assert.NotNull(progress);
                Assert.Equal("InProgress", json["Status"].ToString());
            }
        }

        [Fact]
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

            using (var context = new JsonOperationContext(1024, 1024, SharedMultipleUseFlag.None))
            {
                var json = context.ReadObject(state.ToJson(), "state");
                var result = json["Result"] as BlittableJsonReaderObject;
                Assert.NotNull(result);
                Assert.Equal("Done", result["Message"].ToString());
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
        }
    }
}
