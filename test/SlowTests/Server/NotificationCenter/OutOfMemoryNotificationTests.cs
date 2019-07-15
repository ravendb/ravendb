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
using Xunit;

namespace SlowTests.Server.NotificationCenter
{
    public class OutOfMemoryNotificationTests : RavenLowLevelTestBase
    {
        [Fact]
        public void Add_WhileAllHaveTheSameKey_ShouldRemindOnlyOne()
        {
            using (var database = CreateDocumentDatabase())
            {
                var actions = new AsyncQueue<DynamicJsonValue>();

                var writer = new TestWebSocketWriter();
                var taskList = new List<Task>();
                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    taskList.Add(Task.Run( () => database.NotificationCenter.OutOfMemory.Add("SomeTitle", "SomeKey", new OutOfMemoryException())));
                    taskList.Add(Task.Run( () => database.NotificationCenter.OutOfMemory.Add("SomeTitle", "SomeKey", new OutOfMemoryException())));
                    taskList.Add(Task.Run( () => database.NotificationCenter.OutOfMemory.Add("SomeTitle", "SomeKey", new OutOfMemoryException())));

                    Task.WaitAll(taskList.ToArray());
                }

                using (database.NotificationCenter.GetStored(out var notification))
                {
                    var jsonAlerts = notification.ToList();

                    Assert.Equal(1, jsonAlerts.Count);
                    Assert.Equal("SomeTitle", jsonAlerts[0].Json["Title"].ToString());
                    Assert.Equal("SomeKey", jsonAlerts[0].Json["Key"].ToString());
                }
            }
        }

        [Fact]
        public void Add_WhileNotificationsHaveTwoDifferentKeys_ShouldRemindTwo()
        {
            using (var database = CreateDocumentDatabase())
            {
                var actions = new AsyncQueue<DynamicJsonValue>();

                var writer = new TestWebSocketWriter();
                var taskList = new List<Task>();
                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    for (var i = 0; i < 3; i++)
                    {
                        taskList.Add(Task.Run(() => database.NotificationCenter.OutOfMemory.Add("Title1", "Key1", new OutOfMemoryException())));
                        taskList.Add(Task.Run(() => database.NotificationCenter.OutOfMemory.Add("Title2", "Key2", new OutOfMemoryException())));
                    }

                    Task.WaitAll(taskList.ToArray());
                }

                using (database.NotificationCenter.GetStored(out var notifications))
                {
                    Assert.Equal(2, notifications.Count());

                    var titles = notifications.Select(n => n.Json["Title"].ToString()).ToArray();

                    Assert.Contains("Title1", titles);
                    Assert.Contains("Title2", titles);
                }
            }
        }


        [Fact]
        public async Task Add_WhileAllHaveTheSameKeyAndOneOfterUpdateFrequencyTime_ShouldBeTheLater()
        {
            using (var database = CreateDocumentDatabase())
            {
                var actions = new AsyncQueue<DynamicJsonValue>();

                var writer = new TestWebSocketWriter();
                var taskList = new List<Task>();
                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    taskList.Add(Task.Run( () => database.NotificationCenter.OutOfMemory.Add("FirsTitle1", "SomeKey", new OutOfMemoryException())));
                    taskList.Add(Task.Run( () => database.NotificationCenter.OutOfMemory.Add("FirsTitle2", "SomeKey", new OutOfMemoryException())));
                    taskList.Add(Task.Run( () => database.NotificationCenter.OutOfMemory.Add("FirsTitle3", "SomeKey", new OutOfMemoryException())));

                    await Task.Delay(16_000);
                    
                    taskList.Add(Task.Run( () => database.NotificationCenter.OutOfMemory.Add("SecondTitle", "SomeKey", new OutOfMemoryException())));

                    Task.WaitAll(taskList.ToArray());
                }

                using (database.NotificationCenter.GetStored(out var notification))
                {
                    var jsonAlerts = notification.ToList();

                    Assert.Equal(1, jsonAlerts.Count);
                    Assert.Equal("SecondTitle", jsonAlerts[0].Json["Title"].ToString());
                    Assert.Equal("SomeKey", jsonAlerts[0].Json["Key"].ToString());
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
