// -----------------------------------------------------------------------
//  <copyright file="Bugs.cs" company="Hibernating Rhinos LTD">
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
	public class NotificationOnWrongDatabase : RavenTest
	{
		public class Item
		{
		}

		[Fact]
		public void ShouldNotCrashServer()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079",
				Conventions =
					{
						FailoverBehavior = FailoverBehavior.FailImmediately
					}
			})
			{
				store.Initialize();
				var taskObservable = store.Changes("does-not-exists");
				Assert.Throws<AggregateException>(() => taskObservable.Task.Wait());
				// ensure the db still works
				store.DatabaseCommands.GetStatistics();
			}
		}
	}
}