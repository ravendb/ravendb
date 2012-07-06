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
				var list = new BlockingCollection<ChangeNotification>();
				store.Changes(changes: ChangeTypes.IndexUpdated)
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new ClientServer.Item(), "items/1");
					session.SaveChanges();
				}

				var changeNotification = list.Take();

				Assert.Equal("Raven/DocumentsByEntityName", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.IndexUpdated);
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
				var list = new BlockingCollection<ChangeNotification>();
				store.Changes(changes: ChangeTypes.Put, idPrefix:"items")
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

				var changeNotification = list.Take();

				Assert.Equal("items/1", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.Put);
			}
		}
	}
}