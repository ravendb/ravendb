// -----------------------------------------------------------------------
//  <copyright file="Notifications.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Documents;
using Microsoft.Silverlight.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using System.Reactive.Linq;
using System;
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
			using (var documentStore = new DocumentStore
			{
				Url = Url + Port,
			}.Initialize())
			{
				yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

				var taskObservable = documentStore.Changes(dbname);

				yield return taskObservable.Task;

				var observableWithTask = taskObservable.ForDocument("companies/1");

				yield return observableWithTask.Task;

				observableWithTask
					.Subscribe(tcs.SetResult);

				var entity1 = new Company { Name = "Async Company #1" };
				using (var session_for_storing = documentStore.OpenAsyncSession(dbname))
				{
					session_for_storing.Store(entity1,"companies/1");
					yield return session_for_storing.SaveChangesAsync();
				}

				Task.Factory.StartNew(() =>
				{
					Thread.Sleep(5000);
					tcs.TrySetCanceled();
				});

				yield return tcs.Task;

				Assert.AreEqual("companies/1", tcs.Task.Result.Name);
			}
		}
	}
}