using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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

                var notifications = new BlockingCollection<OperationStatusChangeNotification>();
                var mre = new ManualResetEventSlim(false);

                var operationId = db.Operations.GetNextOperationId();

                db.Notifications.OnOperationStatusChange += notifications.Add;

                db.Operations.AddOperation("Operations Test", (DatabaseOperations.PendingOperationType) 0, 
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
                    }), operationId, token);

                OperationStatusChangeNotification notification;
                Assert.True(notifications.TryTake(out notification, TimeSpan.FromSeconds(1)));
                Assert.NotNull(notification.OperationId);
                Assert.Equal(OperationStatus.InProgress, notification.State.Status);
                Assert.Null(notification.State.Result);
                var progress = notification.State.Progress as DeterminateProgress;
                Assert.NotNull(progress);
                Assert.Equal(1024, progress.Total);
                Assert.Equal(0, progress.Processed);

                mre.Set();

                Assert.True(notifications.TryTake(out notification, TimeSpan.FromSeconds(1)));
                Assert.NotNull(notification.OperationId);
                Assert.Equal(OperationStatus.InProgress, notification.State.Status);
                Assert.Null(notification.State.Result);
                progress = notification.State.Progress as DeterminateProgress;
                Assert.NotNull(progress);
                Assert.Equal(1024, progress.Total);
                Assert.Equal(500, progress.Processed);

                mre.Set();

                Assert.True(notifications.TryTake(out notification, TimeSpan.FromSeconds(1)));
                Assert.NotNull(notification.OperationId);
                Assert.Equal(OperationStatus.Completed, notification.State.Status);
                Assert.NotNull(notification.State.Result);
                Assert.Null(notification.State.Progress);
                var result = notification.State.Result as SampleOperationResult;
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

                var notifications = new BlockingCollection<OperationStatusChangeNotification>();

                db.Notifications.OnOperationStatusChange += notifications.Add;

                db.Operations.AddOperation("Operations Test", (DatabaseOperations.PendingOperationType)0,
                    onProgress => Task.Factory.StartNew<IOperationResult>(() =>
                    {
                       throw new Exception("Something bad happened");
                    }), operationId, OperationCancelToken.None);

                OperationStatusChangeNotification notification;

                Assert.True(notifications.TryTake(out notification, TimeSpan.FromSeconds(1)));
                Assert.NotNull(notification.OperationId);
                Assert.Equal(OperationStatus.Faulted, notification.State.Status);
                Assert.NotNull(notification.State.Result);
                Assert.Null(notification.State.Progress);
                var result = notification.State.Result as OperationExceptionResult;
                Assert.NotNull(result);
                Assert.Equal("Something bad happened", result.Message);
                Assert.IsType<string>(result.StackTrace);
            }
        }

        [Fact]
        public void Should_be_able_to_cancel_operation()
        {
            using (var db = CreateDocumentDatabase())
            {
                var token = new OperationCancelToken(TimeSpan.Zero, CancellationToken.None);
                token.Cancel();

                var notifications = new BlockingCollection<OperationStatusChangeNotification>();

                var operationId = db.Operations.GetNextOperationId();

                db.Notifications.OnOperationStatusChange += notifications.Add;

                db.Operations.AddOperation("Cancellation Test", (DatabaseOperations.PendingOperationType)0,
                    onProgress => Task.Factory.StartNew<IOperationResult>(() =>
                    {
                        token.Token.ThrowIfCancellationRequested();
                        return null;
                    }, token.Token), operationId, token);

                OperationStatusChangeNotification notification;

                Assert.True(notifications.TryTake(out notification, TimeSpan.FromSeconds(1)));
                Assert.NotNull(notification.OperationId);
                Assert.Equal(OperationStatus.Canceled, notification.State.Status);
                Assert.Null(notification.State.Result);
                Assert.Null(notification.State.Progress);
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

            using (var context = new JsonOperationContext(1024, 1024))
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

            using (var context = new JsonOperationContext(1024, 1024))
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
        }
    }
}