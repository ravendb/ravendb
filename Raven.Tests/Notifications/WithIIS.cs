// -----------------------------------------------------------------------
//  <copyright file="WithIIS.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Reactive.Linq;
using Raven.Abstractions.Data;
using Raven.Tests.Bugs.Identifiers;
using Xunit;

namespace Raven.Tests.Notifications
{
	public class WithIIS : IisExpressTestClient
	{
		public class Item
		{
		}

		[IISExpressInstalledFact]
		public void CanHandleCaseSensitivityInProperties()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<ChangeNotification>();
				store.Changes()
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Item(), "items/1");
					session.SaveChanges();
				}

				ChangeNotification changeNotification;
				Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(30)));

				Assert.Equal("items/1", changeNotification.Name);
				Assert.Equal(changeNotification.Type, ChangeTypes.Put);
			}
		}
	}
}