// -----------------------------------------------------------------------
//  <copyright file="BulkInsertTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Silverlight.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Tests.Silverlight
{
	public class BulkInsertTests : RavenTestBase
	{
		[Ignore] // Currently not working, because we can't do unknown legth streamed uploads
		[Asynchronous]
		public IEnumerable<Task> CanCreateAndDisposeUsingBulk()
		{
			using (var store = new DocumentStore { Url = Url + Port }.Initialize())
			{
				using (var op = store.AsyncDatabaseCommands.GetBulkInsertOperation(new BulkInsertOptions(), store.Changes()))
				{
					op.Write("items/1", new RavenJObject(), new RavenJObject());
				}

				using (var session = store.OpenAsyncSession())
				{
					var task = session.LoadAsync<User>("users/1");

					yield return task;

					var user = task.Result;

					Assert.IsNotNull(user);
					Assert.AreEqual("Fitzchak", user.Name);
				}
			}
		}

		private class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}