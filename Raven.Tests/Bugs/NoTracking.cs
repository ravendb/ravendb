// -----------------------------------------------------------------------
//  <copyright file="NoTracking.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class NoTracking : RavenTest
	{
		public class User
		{
			public string Id, Manager;
		}

		[Fact]
		public void AllowLoadingItemsLater()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User { Id = "users/1" });
					s.Store(new User { Id = "users/2", Manager = "users/1" });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var users = s.Advanced.DocumentQuery<User>()
						.Include(x=>x.Manager)
						.WaitForNonStaleResults()
						.NoTracking()
						.WhereEquals(x=>x.Manager, "users/1")
						.ToList();


					Assert.Equal(1, users.Count);
					Assert.True(s.Advanced.IsLoaded("users/1"));
					Assert.False(s.Advanced.IsLoaded("users/2"));

					Assert.NotNull(s.Load<User>("users/2"));
				}
			}
		}
	}
}