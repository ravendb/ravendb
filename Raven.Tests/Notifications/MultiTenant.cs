// -----------------------------------------------------------------------
//  <copyright file="MiultiTenant.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Concurrent;
using System.Reactive.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Xunit;
using System;

namespace Raven.Tests.Notifications
{
	public class MultiTenant : RavenTest
	{
		[Fact]
		public void CanGetNotificationsFromTenant()
		{
			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079",
				DefaultDatabase = "test"
			}.Initialize())
			{
				var list = new BlockingCollection<ChangeNotification>();
				store.Changes()
					.Where(x => x.Type == ChangeType.Put)
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new ClientServer.Item(), "items/1");
					session.SaveChanges();
				}

				var changeNotification = list.Take();

				Assert.Equal("items/1", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeType.Put);
			}

		}
	}
}