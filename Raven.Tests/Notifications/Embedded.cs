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
				var list = new BlockingCollection<ChangeNotification>();
				store.Changes().Subscribe(list.Add);

				using(var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				ChangeNotification changeNotification;
				Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("items/1", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.Put);
			}
		}

		[Fact]
		public void CanGetNotificationAboutDocumentDelete()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<ChangeNotification>();
				store.Changes()
					.Where(x=>x.Type == ChangeTypes.Delete)
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				store.DatabaseCommands.Delete("items/1", null);

				ChangeNotification changeNotification;
				Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("items/1", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.Delete);
			}
		}

		[Fact]
		public void CanGetNotificationAboutDocumentIndexUpdate()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<ChangeNotification>();
				store.Changes()
					.Where(x => x.Type == ChangeTypes.IndexUpdated)
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				store.DatabaseCommands.Delete("items/1", null);

				ChangeNotification changeNotification;
				Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("Raven/DocumentsByEntityName", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.IndexUpdated);
			}
		}
	}
}