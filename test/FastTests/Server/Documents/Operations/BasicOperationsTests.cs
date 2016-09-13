using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Data;
using Raven.Json.Linq;
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
        public async Task Can_notify_about_operations_progress_and_completion()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var db = CreateDocumentDatabase())
            {
                var token = new OperationCancelToken(TimeSpan.FromMinutes(2), CancellationToken.None);

                var notifications = new BlockingCollection<OperationStatusChangeNotification>();
                var mre = new ManualResetEventSlim(false);
                var ws = new FakeWebSocket();

                ws.OnMessageSent += message =>
                {
                    var msgAsObject = message.DeserializeMessage<OperationStatusChangeNotification>();
                    notifications.Add(msgAsObject);
                };

                var runningOperation = db.DatabaseOperations.ExecuteOperation("Operations Test", (DatabaseOperations.PendingOperationType) 0, context,
                    onProgress => Task.Factory.StartNew<IOperationResult>(() =>
                    {
                        mre.Wait(token.Token);
                        mre.Reset();

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
                    }), ws, token);

                OperationStatusChangeNotification notification;
                Assert.True(notifications.TryTake(out notification, TimeSpan.FromSeconds(10)));
                Assert.NotNull(notification.OperationId);
                Assert.Equal(OperationStatus.InProgress, notification.State.Status);
                Assert.Null(notification.State.Result);
                Assert.Null(notification.State.Progress);

                mre.Set();

                Assert.True(notifications.TryTake(out notification, TimeSpan.FromSeconds(10)));
                Assert.NotNull(notification.OperationId);
                Assert.Equal(OperationStatus.InProgress, notification.State.Status);
                Assert.Null(notification.State.Result);
                var progress = notification.State.Progress as DeterminateProgress;
                Assert.NotNull(progress);
                Assert.Equal(1024, progress.Total);
                Assert.Equal(0, progress.Processed);

                mre.Set();

                Assert.True(notifications.TryTake(out notification, TimeSpan.FromSeconds(10)));
                Assert.NotNull(notification.OperationId);
                Assert.Equal(OperationStatus.InProgress, notification.State.Status);
                Assert.Null(notification.State.Result);
                progress = notification.State.Progress as DeterminateProgress;
                Assert.NotNull(progress);
                Assert.Equal(1024, progress.Total);
                Assert.Equal(500, progress.Processed);

                mre.Set();

                Assert.True(notifications.TryTake(out notification, TimeSpan.FromSeconds(10)));
                Assert.NotNull(notification.OperationId);
                Assert.Equal(OperationStatus.Completed, notification.State.Status);
                Assert.NotNull(notification.State.Result);
                Assert.Null(notification.State.Progress);
                var result = notification.State.Result as SampleOperationResult;
                Assert.NotNull(result);
                Assert.Equal("I'm done", result.Message);

                await runningOperation;
            }
        }

        [Fact]
        public void Can_notify_about_exception_in_operation()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var db = CreateDocumentDatabase())
            {
                var notifications = new BlockingCollection<OperationStatusChangeNotification>();

                var ws = new FakeWebSocket();

                ws.OnMessageSent += message =>
                {
                    var msgAsObject = message.DeserializeMessage<OperationStatusChangeNotification>();
                    notifications.Add(msgAsObject);
                };

                var runningOperation = db.DatabaseOperations.ExecuteOperation("Operations Test", (DatabaseOperations.PendingOperationType)0, context,
                    onProgress => Task.Factory.StartNew<IOperationResult>(() =>
                    {
                       throw new Exception("Something bad happened");
                    }), ws, OperationCancelToken.None);


                OperationStatusChangeNotification notification;

                // ignore first message (send to obtain notification id)
                Assert.True(notifications.TryTake(out notification, TimeSpan.FromSeconds(5)));

                Assert.True(notifications.TryTake(out notification, TimeSpan.FromSeconds(5)));
                Assert.NotNull(notification.OperationId);
                Assert.Equal(OperationStatus.Faulted, notification.State.Status);
                Assert.NotNull(notification.State.Result);
                Assert.Null(notification.State.Progress);
                var result = notification.State.Result as OperationExceptionResult;
                Assert.NotNull(result);
                Assert.Equal("Something bad happened", result.Message);
                Assert.IsType<string>(result.StackTrace);

                Assert.Throws<AggregateException>(() => runningOperation.Result);
            }
        }

        [Fact]
        public void Should_be_able_to_cancel_operation()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var db = CreateDocumentDatabase())
            {
                var token = new OperationCancelToken(TimeSpan.FromMinutes(2), CancellationToken.None);

                var notifications = new BlockingCollection<OperationStatusChangeNotification>();

                var ws = new FakeWebSocket();

                ws.OnMessageSent += message =>
                {
                    var msgAsObject = message.DeserializeMessage<OperationStatusChangeNotification>();
                    notifications.Add(msgAsObject);
                };

                var runningOperation = db.DatabaseOperations.ExecuteOperation("Cancellation Test", (DatabaseOperations.PendingOperationType)0, context,
                    onProgress => Task.Factory.StartNew<IOperationResult>(() =>
                    {
                        while (true)
                        {
                            token.Token.ThrowIfCancellationRequested();
                            Thread.Sleep(100);
                        }
                    }, token.Token), ws, token);

                OperationStatusChangeNotification initialNotification;

                Assert.True(notifications.TryTake(out initialNotification, TimeSpan.FromSeconds(5)));
                Assert.NotNull(initialNotification.OperationId);

                db.DatabaseOperations.KillRunningOperation(initialNotification.OperationId);

                OperationStatusChangeNotification afterCancelationNotification;
                Assert.True(notifications.TryTake(out afterCancelationNotification, TimeSpan.FromSeconds(5)));
                Assert.NotNull(afterCancelationNotification.OperationId);
                Assert.Equal(OperationStatus.Canceled, afterCancelationNotification.State.Status);
                Assert.Null(afterCancelationNotification.State.Result);
                Assert.Null(afterCancelationNotification.State.Progress);

                Assert.Throws<AggregateException>(() => runningOperation.Result);
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