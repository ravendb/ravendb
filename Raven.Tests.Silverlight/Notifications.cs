// -----------------------------------------------------------------------
//  <copyright file="Notifications.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Silverlight.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Tests.Document;

namespace Raven.Tests.Silverlight
{
	public class Notifications : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> ShouldGetNotifications()
		{
			var dbname = GenerateNewDatabaseName();
			
			var tcs = new TaskCompletionSource<DocumentChangeNotification>();
			using (var documentStore = NewDocumentStore())
			{
				yield return documentStore.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(dbname);

				var changes = documentStore.Changes(dbname);
				yield return changes.Task;

				var observableWithTask = changes.ForDocument("companies/1");
				yield return observableWithTask.Task;

				observableWithTask
					.Subscribe(tcs.SetResult);

				var entity1 = new Company { Name = "Async Company #1" };
				using (var session_for_storing = documentStore.OpenAsyncSession(dbname))
				{
					yield return session_for_storing.StoreAsync(entity1, "companies/1");
					yield return session_for_storing.SaveChangesAsync();
				}

				Task.Factory.StartNew(() =>
				{
					Thread.Sleep(5000);
					tcs.TrySetCanceled();
				});

				yield return tcs.Task;

				Assert.AreEqual("companies/1", tcs.Task.Result.Id);
			}
		}
	}
}