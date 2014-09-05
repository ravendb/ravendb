// -----------------------------------------------------------------------
//  <copyright file="SortingAsMaxInt.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SortingAsMaxInt : RavenTest
	{
		public class User
		{
			public long Age;
		}

		public class Users_ByAge : AbstractIndexCreationTask<User>
		{
			public Users_ByAge()
			{
				Map = users =>
					from user in users
					select new {user.Age};
				Sort(x => x.Age, SortOptions.Int);
			}
		}

		[Fact]
		public void CanSortOnMaxInt()
		{
			using (var store = NewDocumentStore())
			{
				new Users_ByAge().Execute(store);

				using (var s = store.OpenSession())
				{
					s.Store(new User{Age = 182});
					s.Store(new User { Age = ((long)int.MaxValue)+1L});
					s.SaveChanges();
				}
				WaitForIndexing(store);

				using (var s = store.OpenSession())
				{
					var a = s.Query<User, Users_ByAge>()
						.OrderBy(x => x.Age)
						.ToList();

					Assert.Equal(182, a[0].Age);
					Assert.Equal(int.MaxValue+1L, a[1].Age);
				}
			}
		}
	}
}