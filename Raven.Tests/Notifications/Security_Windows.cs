// -----------------------------------------------------------------------
//  <copyright file="Security.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Reactive.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Server;
using Xunit;

namespace Raven.Tests.Notifications
{
	public class Security_Windows : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
		}

		[Fact]
		public void WithWindowsAuth()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079",
			}.Initialize())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();
				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				var documentSubscription = taskObservable.ForDocument("items/1");
				documentSubscription.Task.Wait();
				documentSubscription
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new ClientServer.Item(), "items/1");
					session.SaveChanges();
				}

				DocumentChangeNotification changeNotification;
				Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("items/1", changeNotification.Id);
				Assert.Equal(changeNotification.Type, DocumentChangeTypes.Put);
			}
		}
	}
}