// -----------------------------------------------------------------------
//  <copyright file="ReplicationConflicts_Embedded.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Notifications
{
	public class ReplicationConflicts_Embedded : RavenTest
	{

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.Settings.Add("Raven/ActiveBundles", "replication");
			configuration.PostInit();
		}

		[Fact]
		public void CanGetNotificationsAboutConflictedDocuments()
		{
			using (GetNewServer(port: 8079))
			using (var documentStore = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{

				using (var embeddableStore = new EmbeddableDocumentStore
				{
					UseEmbeddedHttpServer = true,
					Configuration =
					{
						Port = 8078,
						Settings = { {"Raven/ActiveBundles", "replication"} }
					},
					RunInMemory = true,
					
				}.Initialize())
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

					// setup and run replication
					using (var session = documentStore.OpenSession())
					{
						var replicationDestination = new ReplicationDestination
						{
							Url = "http://localhost:8078",

						};

						session.Store(new ReplicationDocument
						{
							Destinations = { replicationDestination }
						}, "Raven/Replication/Destinations");
						session.SaveChanges();
					}

					ReplicationConflictNotification replicationConflictNotification;
					Assert.True(list.TryTake(out replicationConflictNotification, TimeSpan.FromSeconds(10 * 600)));

					Assert.Equal("users/1", replicationConflictNotification.Id);
					Assert.Equal(replicationConflictNotification.Type, ReplicationConflictTypes.DocumentReplicationConflict);

				}
			}
		}
	}
}