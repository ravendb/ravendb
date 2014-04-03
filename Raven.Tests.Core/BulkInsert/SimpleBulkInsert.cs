// -----------------------------------------------------------------------
//  <copyright file="SimpleBulkInsert.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
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

		[Fact]
		public void BulkInsertShouldNotOverwriteWithOverwriteExistingSetToFalse()
		{
			using (var store = GetDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User
					              {
						              Id = "users/1", 
									  Name = "User - 1"
					              });

					session.SaveChanges();
				}

				var e = Assert.Throws<ConcurrencyException>(() =>
				{
					using (var bulkInsert = store.BulkInsert(options: new BulkInsertOptions { OverwriteExisting = false }))
					{
						for (var i = 0; i < 10; i++)
						{
							bulkInsert.Store(new User
							                 {
								                 Id = "users/" + (i + 1), 
												 Name = "resU - " + (i + 1)
							                 });
						}
					}
				});

				Assert.Contains("'users/1' already exists", e.Message);

				using (var session = store.OpenSession())
				{
					var users = session.Advanced.LoadStartingWith<User>("users/", pageSize: 128);
					Assert.Equal(1, users.Length);
					Assert.True(users.All(x => x.Name.StartsWith("User")));
				}
			}
		}

		[Fact]
		public void BulkInsertShouldOverwriteWithOverwriteExistingSetToTrue()
		{
			using (var store = GetDocumentStore())
			{
				using (var bulkInsert = store.BulkInsert())
				{
					for (int i = 0; i < 10; i++)
					{
						bulkInsert.Store(new User
						                 {
							                 Id = "users/" + (i + 1), 
											 Name = "User - " + (i + 1)
						                 });
					}
				}

				using (var bulkInsert = store.BulkInsert(options: new BulkInsertOptions { OverwriteExisting = true }))
				{
					for (int i = 0; i < 10; i++)
					{
						bulkInsert.Store(new User
						                 {
							                 Id = "users/" + (i + 1), 
											 Name = "resU - " + (i + 1)
						                 });
					}
				}

				using (var session = store.OpenSession())
				{
					var users = session.Advanced.LoadStartingWith<User>("users/", pageSize: 128);
					Assert.Equal(10, users.Length);
					Assert.True(users.All(x => x.Name.StartsWith("resU")));
				}
			}
		}
	}
}