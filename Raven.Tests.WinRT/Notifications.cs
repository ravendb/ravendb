// -----------------------------------------------------------------------
//  <copyright file="Notifications.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Tests.Document;

namespace Raven.Tests.WinRT
{
	public class Notifications : RavenTestBase
	{
		[TestMethod]
		public async Task ShouldGetNotifications()
		{
			var dbname = GenerateNewDatabaseName("Notifications.ShouldGetNotifications");
			
			var tcs = new TaskCompletionSource<DocumentChangeNotification>();
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var taskObservable = store.Changes(dbname);
				await taskObservable.Task;

				var observableWithTask = taskObservable.ForDocument("companies/1");
				await observableWithTask.Task;

				/*observableWithTask.Subscribe(tcs.SetResult);*/

				var entity1 = new Company { Name = "Async Company #1" };
				using (var session_for_storing = store.OpenAsyncSession(dbname))
				{
					await session_for_storing.StoreAsync(entity1,"companies/1");
					await session_for_storing.SaveChangesAsync();
				}

				Task.Factory.StartNew(() =>
				{
					Task.Delay(5000).Wait();
					tcs.TrySetCanceled();
				});

				await tcs.Task;

				Assert.AreEqual("companies/1", tcs.Task.Result.Id);
			}
		}
	}
}