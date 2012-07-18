// -----------------------------------------------------------------------
//  <copyright file="Embedded.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Concurrent;
using Raven.Abstractions.Data;
using Xunit;
using System.Reactive.Linq;
using System;

namespace Raven.Tests.Notifications
{
	public class Embedded : RavenTest
	{
		public class Item
		{
		}

		[Fact]
		public void CanGetNotificationAboutDocumentPut()
		{
			using(var store = NewDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();
				store.Changes()
					.DocumentSubscription("items/1")
					.Subscribe(list.Add);

				using(var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("items/1", documentChangeNotification.Name);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}

		[Fact]
		public void CanGetNotificationAboutDocumentDelete()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();
				store.Changes()
					.DocumentSubscription("items/1")
					.Where(x=>x.Type == DocumentChangeTypes.Delete)
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				store.DatabaseCommands.Delete("items/1", null);

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("items/1", documentChangeNotification.Name);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Delete);
			}
		}

		[Fact]
		public void CanGetNotificationAboutDocumentIndexUpdate()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<IndexChangeNotification>();
				store.Changes()
					.IndexSubscription("Raven/DocumentsByEntityName")
					.Where(x=>x.Type==IndexChangeTypes.MapCompleted)
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				store.DatabaseCommands.Delete("items/1", null);

				IndexChangeNotification changeNotification;
				Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("Raven/DocumentsByEntityName", changeNotification.Name);
				Assert.Equal(changeNotification.Type, IndexChangeTypes.MapCompleted);
			}
		}
	}
}