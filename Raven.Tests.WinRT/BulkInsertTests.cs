// -----------------------------------------------------------------------
//  <copyright file="BulkInsertTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class BulkInsertTests : RavenTestBase
	{
		[Ignore] // Currently not working, because we can't do unknown length streamed uploads
		[TestMethod]
		public async Task CanCreateAndDisposeUsingBulk()
		{
			using (var store = NewDocumentStore())
			{
				/*using (var op = store.AsyncDatabaseCommands.GetBulkInsertOperation(new BulkInsertOptions(), store.Changes()))
				{
					op.Write("items/1", new RavenJObject(), new RavenJObject());
				}*/

				using (var session = store.OpenAsyncSession())
				{
					var user = await session.LoadAsync<User>("users/1");

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