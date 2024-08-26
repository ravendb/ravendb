using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.NotificationCenter
{
    public class OutOfMemoryNotificationTests : RavenLowLevelTestBase
    {
        public OutOfMemoryNotificationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Add_WhenCalled_ShouldNotPreventFromGarbageCollectorToCollectTheEnvironmentObject()
        {
            using (var database = CreateDocumentDatabase())
            {
                WeakReference weakReference;
                using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(RavenTestHelper.NewDataPath(nameof(OutOfMemoryNotificationTests), 0))))
                {
                    weakReference = new WeakReference(env);
                    database.NotificationCenter.OutOfMemory.Add(env, new OutOfMemoryException());
                    Assert.NotNull(weakReference.Target);
                }

                for (var i = 0; i < 20; i++)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    GC.Collect();
                    GC.WaitForPendingFinalizers();

                    if (weakReference.Target == null)
                        break;
                }

                Assert.Null(weakReference.Target);
            }
        }

        [Fact]
        public async Task Add_WhileAllHaveTheSameKey_ShouldRemindOnlyOne()
        {
            using (var database = CreateDocumentDatabase())
            {
                var environment = database.GetAllStoragesEnvironment().First().Environment;

                var actions = new AsyncQueue<DynamicJsonValue>();

                var writer = new TestWebSocketWriter();
                var taskList = new List<Task>();
                using (database.NotificationCenter.TrackActions(actions, writer))
                {

                    taskList.Add(Task.Run( () => database.NotificationCenter.OutOfMemory.Add(environment, new OutOfMemoryException())));
                    taskList.Add(Task.Run( () => database.NotificationCenter.OutOfMemory.Add(environment, new OutOfMemoryException())));
                    taskList.Add(Task.Run( () => database.NotificationCenter.OutOfMemory.Add(environment, new OutOfMemoryException())));

                    await Task.WhenAll(taskList.ToArray());
                }

                using (database.NotificationCenter.GetStored(out var notifications))
                {
                    Assert.Equal(1, notifications.Count());
                    Assert.Equal($"Out of memory occurred for '{environment}'", notifications.First().Json["Title"].ToString());
                    Assert.Equal($"{environment}:{typeof(OutOfMemoryException)}", notifications.First().Json["Key"].ToString());
                }
            }
        }

        [Fact]
        public async Task Add_WhileNotificationsAreForDifferentEnvironments_ShouldRemindTwo()
        {
            using (var database = CreateDocumentDatabase())
            {
                var environments = database.GetAllStoragesEnvironment().ToArray();

                var firstEnvironment = environments[0].Environment;
                var secondEnvironment = environments[1].Environment;

                var actions = new AsyncQueue<DynamicJsonValue>();

                var writer = new TestWebSocketWriter();
                var taskList = new List<Task>();
                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    for (var i = 0; i < 3; i++)
                    {
                        taskList.Add(Task.Run(() => database.NotificationCenter.OutOfMemory.Add(firstEnvironment, new OutOfMemoryException())));
                        taskList.Add(Task.Run(() => database.NotificationCenter.OutOfMemory.Add(secondEnvironment, new OutOfMemoryException())));
                    }

                    await Task.WhenAll(taskList.ToArray());
                }

                using (database.NotificationCenter.GetStored(out var notifications))
                {
                    Assert.Equal(2, notifications.Count());

                    var keys = notifications.Select(n => n.Json["Key"].ToString()).ToArray();

                    Assert.Contains($"{firstEnvironment}:{typeof(OutOfMemoryException)}", keys);
                    Assert.Contains($"{secondEnvironment}:{typeof(OutOfMemoryException)}", keys);
                }
            }
        }

        [Fact]
        public async Task Add_WhileNotificationsHaveDifferentExceptionTypeInSameEnvironment_ShouldRemindTwo()
        {
            using (var database = CreateDocumentDatabase())
            {
                var environment = database.GetAllStoragesEnvironment().First().Environment;

                var actions = new AsyncQueue<DynamicJsonValue>();

                var writer = new TestWebSocketWriter();
                var taskList = new List<Task>();
                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    for (var i = 0; i < 3; i++)
                    {
                        taskList.Add(Task.Run(() => database.NotificationCenter.OutOfMemory.Add(environment, new OutOfMemoryException())));
                        taskList.Add(Task.Run(() => database.NotificationCenter.OutOfMemory.Add(environment, new InsufficientMemoryException())));
                    }

                    await Task.WhenAll(taskList.ToArray());
                }

                using (database.NotificationCenter.GetStored(out var notifications))
                {
                    Assert.Equal(2, notifications.Count());

                    var keys = notifications.Select(n => n.Json["Key"].ToString()).ToArray();

                    Assert.Contains($"{environment}:{typeof(OutOfMemoryException)}", keys);
                    Assert.Contains($"{environment}:{typeof(InsufficientMemoryException)}", keys);
                }
            }
        }


        [Fact]
        public async Task Add_WhileAllHaveTheSameKeyAndOneOfterUpdateFrequencyTime_ShouldBeTheLater()
        {
            using (var database = CreateDocumentDatabase())
            {
                var environment = database.GetAllStoragesEnvironment().First().Environment;

                var actions = new AsyncQueue<DynamicJsonValue>();

                var writer = new TestWebSocketWriter();
                var taskList = new List<Task>();
                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    taskList.Add(Task.Run( () => database.NotificationCenter.OutOfMemory.Add(environment, new Exception("First Message"))));

                    await Task.Delay(16_000);

                    taskList.Add(Task.Run(() => database.NotificationCenter.OutOfMemory.Add(environment, new Exception("Second Message"))));

                    await Task.WhenAll(taskList.ToArray());
                }

                using (database.NotificationCenter.GetStored(out var notifications))
                {
                    var jsonAlerts = notifications.ToList();

                    Assert.Equal(1, notifications.Count());
                    Assert.Equal("Second Message", notifications.First().Json["Message"].ToString());
                }
            }
        }
        
        private class TestWebSocketWriter : IWebsocketWriter
        {
            private List<string> SentNotifications { get; } = new List<string>();

            public Task WriteToWebSocket<TNotification>(TNotification notification)
            {
                var blittable = notification as BlittableJsonReaderObject;

                SentNotifications.Add(blittable[nameof(Notification.Id)].ToString());

                return Task.CompletedTask;
            }
        }
    }
}
