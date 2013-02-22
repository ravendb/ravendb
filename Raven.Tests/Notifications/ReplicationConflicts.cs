// -----------------------------------------------------------------------
//  <copyright file="ReplicationConflicts.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
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
				Assert.True(list.TryTake(out replicationConflictNotification, TimeSpan.FromSeconds(10 * 600)));

				Assert.Equal("users/1", replicationConflictNotification.Id);
				Assert.Equal(replicationConflictNotification.Type, ReplicationConflictTypes.DocumentReplicationConflict);
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

				store2.DatabaseCommands.PutAttachment("attachment/1", null, new MemoryStream(new byte[] {1, 2, 3}),
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
				Assert.Equal(replicationConflictNotification.Type, ReplicationConflictTypes.AttachmentReplicationConflict);
			}
		}
	}
}