// -----------------------------------------------------------------------
//  <copyright file="ReplicationConflicts_Embedded.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using Raven.Abstractions.Data;
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
			using (var documentStore = CreateStore())
			{
				using (var embeddableStore = CreateEmbeddableStore())
				{
					documentStore.DatabaseCommands.Put("users/1", null, new RavenJObject
					{
						{"Name", "Ayende"}
					}, new RavenJObject());

					embeddableStore.DatabaseCommands.Put("users/1", null, new RavenJObject
					{
						{"Name", "Rahien"}
					}, new RavenJObject());

					var list = new BlockingCollection<ReplicationConflictNotification>();
					var taskObservable = embeddableStore.Changes();
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
			using (var documentStore = CreateStore())
			{
				using (var embeddableStore = CreateEmbeddableStore())
				{
					documentStore.DatabaseCommands.Put("users/1", null, new RavenJObject
					{
						{"Name", "Ayende"}
					}, new RavenJObject());

					embeddableStore.DatabaseCommands.Put("users/1", null, new RavenJObject
					{
						{"Name", "Rahien"}
					}, new RavenJObject());

					documentStore.DatabaseCommands.Delete("users/1", null);

					var list = new BlockingCollection<ReplicationConflictNotification>();
					var taskObservable = embeddableStore.Changes();
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
	}
}