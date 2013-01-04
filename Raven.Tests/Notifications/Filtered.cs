// -----------------------------------------------------------------------
//  <copyright file="Filtered.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Reactive.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Notifications
{
	public class Filtered : RavenTest
	{
		[Fact]
		public void CanGetNotificationAboutIndexUpdate()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var list = new BlockingCollection<IndexChangeNotification>();
				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				taskObservable
					.ForIndex("Raven/DocumentsByEntityName")
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new ClientServer.Item(), "items/1");
					session.SaveChanges();
				}

				IndexChangeNotification changeNotification;
				Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("Raven/DocumentsByEntityName", changeNotification.Name);
				Assert.Equal(changeNotification.Type, IndexChangeTypes.MapCompleted);
			}
		}

		[Fact]
		public void CanGetById()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();
				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				var documentPrefixSubscription = taskObservable.ForDocumentsStartingWith("items");
				documentPrefixSubscription.Task.Wait();
				documentPrefixSubscription
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new ClientServer.Item(), "seeks/1");
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Store(new ClientServer.Item(), "items/1");
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("items/1", documentChangeNotification.Id);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}
	}
}