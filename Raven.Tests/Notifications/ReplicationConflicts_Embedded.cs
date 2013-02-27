// -----------------------------------------------------------------------
//  <copyright file="ReplicationConflicts_Embedded.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Xunit;

namespace Raven.Tests.Notifications
{
	public class ReplicationConflicts_Embedded : ReplicationBase
	{
		[Fact]
		public void CanGetNotificationsAboutConflictedDocuments()
		{
			PortRangeStart = 8079;

			using (var store1 = CreateEmbeddableStore())
			{
				using (var store2 = CreateEmbeddableStore())
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
				}
			}
		}

		[Fact]
		public void CanGetNotificationsConflictedDocumentsCausedByDelete()
		{
			PortRangeStart = 8069;

			using (var store1 = CreateEmbeddableStore())
			{
				using (var store2 = CreateEmbeddableStore())
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
		}

		[Fact]
		public void ConflictShouldBeResolvedByRegisiteredConflictListenerWhenNotificationArrives()
		{
			PortRangeStart = 8059;

			using (var store1 = CreateEmbeddableStore())
			using (var store2 = CreateEmbeddableStore())
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

				store2.RegisterListener(new ClientSideConflictResolution());

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