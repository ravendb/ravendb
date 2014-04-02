// -----------------------------------------------------------------------
//  <copyright file="SimpleBulkInsert.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Core.BulkInsert
{
	public class SimpleBulkInsert : RavenCoreTestBase
	{
		[Fact]
		public void BasicBulkInsert()
		{
			using (var store = GetDocumentStore())
			{
				using (var bulkInsert = store.BulkInsert())
				{
					for (int i = 0; i < 100; i++)
					{
						bulkInsert.Store(new User { Name = "User - " + i });
					}
				}


				using (var session = store.OpenSession())
				{
					var users = session.Advanced.LoadStartingWith<User>("users/", pageSize: 128);
					Assert.Equal(100, users.Length);
				}
			}
		}
	}
}