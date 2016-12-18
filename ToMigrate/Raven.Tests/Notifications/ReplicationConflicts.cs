// -----------------------------------------------------------------------
//  <copyright file="ReplicationConflicts.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Notifications
{
    public class ReplicationConflicts : ReplicationBase
    {
        [Fact]
        public void CanGetNotificationsAboutConflictedDocuments()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store1.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Ayende"}
                }, new RavenJObject());

                store2.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Rahien"}
                }, new RavenJObject());

                var list = new BlockingCollection<ReplicationConflictNotification>();
                var taskObservable = store2.Changes();
                taskObservable.Task.Wait();
                var observableWithTask = taskObservable.ForAllReplicationConflicts();
                observableWithTask.Task.Wait();
                observableWithTask
                    .Subscribe(list.Add);

                TellFirstInstanceToReplicateToSecondInstance();

                ReplicationConflictNotification replicationConflictNotification;
                Assert.True(list.TryTake(out replicationConflictNotification, TimeSpan.FromSeconds(10)));

                Assert.Equal("users/1", replicationConflictNotification.Id);
                Assert.Equal(replicationConflictNotification.ItemType, ReplicationConflictTypes.DocumentReplicationConflict);
                Assert.Equal(2, replicationConflictNotification.Conflicts.Length);
                Assert.Equal(ReplicationOperationTypes.Put, replicationConflictNotification.OperationType);

                Etag conflictedEtag = null;

                try
                {
                    store2.DatabaseCommands.Get("users/1");
                }
                catch (ConflictException ex)
                {
                    conflictedEtag = ex.Etag;
                }

                Assert.Equal(conflictedEtag, replicationConflictNotification.Etag);
            }
        }

        [Fact]
        public void CanGetNotificationsAboutConflictedAttachements()
        {
            using(var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store1.DatabaseCommands.PutAttachment("attachment/1", null, new MemoryStream(new byte[] {1, 2, 3}),
                                                      new RavenJObject());

                store2.DatabaseCommands.PutAttachment("attachment/1", null, new MemoryStream(new byte[] {1, 2, 3, 4}),
                                                      new RavenJObject());

                var list = new BlockingCollection<ReplicationConflictNotification>();
                var taskObservable = store2.Changes();
                taskObservable.Task.Wait();
                var observableWithTask = taskObservable.ForAllReplicationConflicts();
                observableWithTask.Task.Wait();
                observableWithTask
                    .Subscribe(list.Add);

                TellFirstInstanceToReplicateToSecondInstance();

                ReplicationConflictNotification replicationConflictNotification;
                Assert.True(list.TryTake(out replicationConflictNotification, TimeSpan.FromSeconds(10)));

                Assert.Equal("attachment/1", replicationConflictNotification.Id);
                Assert.Equal(replicationConflictNotification.ItemType, ReplicationConflictTypes.AttachmentReplicationConflict);
                Assert.Equal(2, replicationConflictNotification.Conflicts.Length);
                Assert.Equal(ReplicationOperationTypes.Put, replicationConflictNotification.OperationType);
            }
        }

        [Fact]
        public void NotificationShouldContainAllConfictedIds()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            using (var store3 = CreateStore())
            {
                store1.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Ayende"}
                }, new RavenJObject());

                store2.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Rahien"}
                }, new RavenJObject());

                store3.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Rahien"}
                }, new RavenJObject());

                var list = new BlockingCollection<ReplicationConflictNotification>();
                var taskObservable = store3.Changes();
                taskObservable.Task.Wait();
                var observableWithTask = taskObservable.ForAllReplicationConflicts();
                observableWithTask.Task.Wait();
                observableWithTask
                    .Subscribe(list.Add);

                TellInstanceToReplicateToAnotherInstance(0, 2); // will create conflict on 3

                ReplicationConflictNotification replicationConflictNotification;
                Assert.True(list.TryTake(out replicationConflictNotification, TimeSpan.FromSeconds(10)));

                Assert.Equal("users/1", replicationConflictNotification.Id);
                Assert.Equal(replicationConflictNotification.ItemType, ReplicationConflictTypes.DocumentReplicationConflict);
                Assert.Equal(2, replicationConflictNotification.Conflicts.Length);

                TellInstanceToReplicateToAnotherInstance(1, 2); // will add another conflicted document on 3

                Assert.True(list.TryTake(out replicationConflictNotification, TimeSpan.FromSeconds(10)));

                Assert.Equal("users/1", replicationConflictNotification.Id);
                Assert.Equal(replicationConflictNotification.ItemType, ReplicationConflictTypes.DocumentReplicationConflict);
                Assert.Equal(3, replicationConflictNotification.Conflicts.Length); // there should be 3 ids of conflicted items
                Assert.Equal(ReplicationOperationTypes.Put, replicationConflictNotification.OperationType);
            }
        }

        [Fact]
        public void CanGetNotificationsWhenDeleteReplicationCausesConflict()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store1.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Ayende"}
                }, new RavenJObject());

                store2.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Rahien"}
                }, new RavenJObject());

                store1.DatabaseCommands.Delete("users/1", null);

                var list = new BlockingCollection<ReplicationConflictNotification>();
                var taskObservable = store2.Changes();
                taskObservable.Task.Wait();
                var observableWithTask = taskObservable.ForAllReplicationConflicts();
                observableWithTask.Task.Wait();
                observableWithTask
                    .Subscribe(list.Add);

                TellFirstInstanceToReplicateToSecondInstance();

                ReplicationConflictNotification replicationConflictNotification;
                Assert.True(list.TryTake(out replicationConflictNotification, TimeSpan.FromSeconds(10)));

                Assert.Equal("users/1", replicationConflictNotification.Id);
                Assert.Equal(replicationConflictNotification.ItemType, ReplicationConflictTypes.DocumentReplicationConflict);
                Assert.Equal(2, replicationConflictNotification.Conflicts.Length);
                Assert.Equal(ReplicationOperationTypes.Delete, replicationConflictNotification.OperationType);
            }
        }

        [Fact]
        public void NotificationShouldContainAllConflictedIdsOfReplicatedDeletes()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            using (var store3 = CreateStore())
            {
                store1.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Ayende"}
                }, new RavenJObject());

                store2.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Rahien"}
                }, new RavenJObject());

                store3.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Rahien"}
                }, new RavenJObject());

                store1.DatabaseCommands.Delete("users/1", null);
                store2.DatabaseCommands.Delete("users/1", null);

                var list = new BlockingCollection<ReplicationConflictNotification>();
                var taskObservable = store3.Changes();
                taskObservable.Task.Wait();
                var observableWithTask = taskObservable.ForAllReplicationConflicts();
                observableWithTask.Task.Wait();
                observableWithTask
                    .Subscribe(list.Add);

                TellInstanceToReplicateToAnotherInstance(0, 2); // will create conflict on 3

                ReplicationConflictNotification replicationConflictNotification;
                Assert.True(list.TryTake(out replicationConflictNotification, TimeSpan.FromSeconds(10)));

                Assert.Equal("users/1", replicationConflictNotification.Id);
                Assert.Equal(replicationConflictNotification.ItemType, ReplicationConflictTypes.DocumentReplicationConflict);
                Assert.Equal(2, replicationConflictNotification.Conflicts.Length);
                Assert.Equal(ReplicationOperationTypes.Delete, replicationConflictNotification.OperationType);

                TellInstanceToReplicateToAnotherInstance(1, 2); // will add another conflicted of deleted document on 3

                Assert.True(list.TryTake(out replicationConflictNotification, TimeSpan.FromSeconds(10)));

                Assert.Equal("users/1", replicationConflictNotification.Id);
                Assert.Equal(replicationConflictNotification.ItemType, ReplicationConflictTypes.DocumentReplicationConflict);
                Assert.Equal(3, replicationConflictNotification.Conflicts.Length); // there should be 3 ids of conflicted items
                Assert.Equal(ReplicationOperationTypes.Delete, replicationConflictNotification.OperationType);
            }
        }

        [Fact]
        public void ConflictShouldBeResolvedByRegisiteredConflictListenerWhenNotificationArrives()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store2.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;

                store1.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Ayende"}
                }, new RavenJObject());

                store2.DatabaseCommands.Put("users/1", null, new RavenJObject
                {
                    {"Name", "Rahien"}
                }, new RavenJObject());

                ((DocumentStore)store2).RegisterListener(new ClientSideConflictResolution());

                var list = new BlockingCollection<ReplicationConflictNotification>();
                var taskObservable = store2.Changes();
                taskObservable.Task.Wait();
                var observableWithTask = taskObservable.ForAllReplicationConflicts();
                observableWithTask.Task.Wait();
                observableWithTask
                    .Subscribe(list.Add);

                TellFirstInstanceToReplicateToSecondInstance();

                ReplicationConflictNotification replicationConflictNotification;
                Assert.True(list.TryTake(out replicationConflictNotification, TimeSpan.FromSeconds(10)));

                var conflictedDocumentsDeleted = false;

                for (int i = 0; i < RetriesCount; i++)
                {
                    var document1 = store2.DatabaseCommands.Get(replicationConflictNotification.Conflicts[0]);
                    var document2 = store2.DatabaseCommands.Get(replicationConflictNotification.Conflicts[1]);

                    if (document1 == null && document2 == null)
                    {
                        conflictedDocumentsDeleted = true;
                        break;
                    }

                    Thread.Sleep(200);
                }

                Assert.True(conflictedDocumentsDeleted);

                var jsonDocument = store2.DatabaseCommands.Get("users/1");

                Assert.Equal("Ayende Rahien", jsonDocument.DataAsJson.Value<string>("Name"));
            }
        }
    }
}
