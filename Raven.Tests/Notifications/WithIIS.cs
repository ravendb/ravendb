// -----------------------------------------------------------------------
//  <copyright file="WithIIS.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Reactive.Linq;
using Raven.Abstractions.Data;
using Raven.Tests.Bugs.Identifiers;
using Xunit;
using Raven.Abstractions.Extensions;

namespace Raven.Tests.Notifications
{
	public class WithIIS : IisExpressTestClient
	{
		public class Item
		{
		}

		[IISExpressInstalledFact]
		public void CheckNotificationInIIS()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();
				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				var observableWithTask = taskObservable.ForDocument("items/1");
				observableWithTask.Task.Wait();
				
				observableWithTask.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(5)));

				Assert.Equal("items/1", documentChangeNotification.Id);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}
	}
}