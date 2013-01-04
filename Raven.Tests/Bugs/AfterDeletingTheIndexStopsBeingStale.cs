//-----------------------------------------------------------------------
// <copyright file="AfterDeletingTheIndexStopsBeingStale.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class AfterDeletingTheIndexStopsBeingStale : RavenTest
	{
		[Fact]
		public void Deletion()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 3; i++)
					{
						session.Store(new User { Name = "Ayende" });
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(20))).ToList();
					Assert.NotEmpty(users);
					foreach (var user in users)
					{
						session.Delete(user);
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(20))).ToList();
					Assert.Empty(users);
				}
			}
		}
	}
}
