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
using Raven.Client.Extensions;

namespace Raven.Tests.Notifications
{
	public class MultiTenant : RavenTest
	{
		[Fact]
		public void CanGetNotificationsFromTenant_DefaultDatabase()
		{
			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079",
				DefaultDatabase = "test"
			}.Initialize())
			{
				var list = new BlockingCollection<ChangeNotification>();
				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				taskObservable
					.Where(x => x.Type == ChangeTypes.Put)
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new ClientServer.Item(), "items/1");
					session.SaveChanges();
				}

				ChangeNotification changeNotification;
				Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(15)));

				Assert.Equal("items/1", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.Put);
			}

		}

		[Fact]
		public void CanGetNotificationsFromTenant_ExplicitDatabase()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079",
			}.Initialize())
			{
				store.DatabaseCommands.EnsureDatabaseExists("test");
				var list = new BlockingCollection<ChangeNotification>();
				var taskObservable = store.Changes("test");
				taskObservable.Task.Wait();
				taskObservable
					.Where(x => x.Type == ChangeTypes.Put)
					.Subscribe(list.Add);

				using (var session = store.OpenSession("test"))
				{
					session.Store(new ClientServer.Item(), "items/1");
					session.SaveChanges();
				}

				ChangeNotification changeNotification;
				Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(15)));

				Assert.Equal("items/1", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.Put);
			}

		}

		[Fact]
		public void CanGetNotificationsFromTenant_AndNotFromAnother()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079",
			}.Initialize())
			{
				store.DatabaseCommands.EnsureDatabaseExists("test");
				store.DatabaseCommands.EnsureDatabaseExists("another");
				var list = new BlockingCollection<ChangeNotification>();
				var taskObservable = store.Changes("test");
				taskObservable.Task.Wait();
				taskObservable
					.Where(x => x.Type == ChangeTypes.Put)
					.Subscribe(list.Add);

				using (var session = store.OpenSession("another"))
				{
					session.Store(new ClientServer.Item(), "items/2");
					session.SaveChanges();
				}

				using (var session = store.OpenSession("test"))
				{
					session.Store(new ClientServer.Item(), "items/1");
					session.SaveChanges();
				}

				ChangeNotification changeNotification;
				Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(15)));

				Assert.Equal("items/1", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.Put);

				Assert.False(list.TryTake(out changeNotification, TimeSpan.FromMilliseconds(250)));
			}
		}
	}
}