// -----------------------------------------------------------------------
//  <copyright file="AsyncSetBasedOps.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bugs
{
	public class AsyncSetBasedOps : RavenTest
	{
		public class User
		{
			public string FirstName;
			public string LastName;
			public string FullName;
		}

		[Theory(Skip = "causes issues")]
		[PropertyData("Storages")]
		public async Task AwaitAsyncPatchByIndexShouldWork(string storageTypeName)
		{
			using (var store = NewRemoteDocumentStore(fiddler:true,requestedStorage:storageTypeName,runInMemory:false))
			{
				string lastUserId = null;

				using (var bulkInsert = store.BulkInsert())
				{
					for (int i = 0; i < 1000 * 10; i++)
					{
						lastUserId = bulkInsert.Store(
							new User
							{
								FirstName = "First #" + i,
								LastName = "Last #" + i
							}
						);
					}					
				}

				while (true)
				{
					var stats = await store.AsyncDatabaseCommands.GetStatisticsAsync();
					
					if (!stats.StaleIndexes.Contains("Raven/DocumentsByEntityName", StringComparer.OrdinalIgnoreCase))
					{
						break;
					}
					await Task.Delay(100);
				}

				await (await store.AsyncDatabaseCommands.UpdateByIndexAsync(
					"Raven/DocumentsByEntityName",
					new IndexQuery { Query = "Tag:Users" },
					new ScriptedPatchRequest
					{
						Script = "this.FullName = this.FirstName + ' ' + this.LastName;"
					}
				))
				.WaitForCompletionAsync();

				using (var db = store.OpenAsyncSession())
				{
					var lastUser = await db.LoadAsync<User>(lastUserId);
					Assert.NotNull(lastUser.FullName);
				}
			}
		}

	}
}