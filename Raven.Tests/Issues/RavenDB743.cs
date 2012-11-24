// -----------------------------------------------------------------------
//  <copyright file="RavenDB743.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB743 : RavenTest
	{
		public class User
		{
			public string Id { get; set; }
			public string Country { get; set; }
			public int Items { get; set; }
		}

		public class UsersByCountry : AbstractIndexCreationTask<User, UsersByCountry.Result>
		{
			public class Result
			{
				public string Country { get; set; }
				public int Count { get; set; }
			}

			public UsersByCountry()
			{
				Map = users =>
				      from user in users
				      select new {user.Country, Count = 1};
				Reduce = results =>
				         from result in results
				         group result by result.Country
				         into g
				         select new
				         {
					         Country = g.Key,
					         Count = g.Sum(x => x.Count)
				         };
			}
		}

		public class ItemssByCountry : AbstractIndexCreationTask<User, UsersByCountry.Result>
		{
			public class Result
			{
				public string Country { get; set; }
				public int Count { get; set; }
			}

			public ItemssByCountry()
			{
				Map = users =>
					  from user in users
					  select new { user.Country, Count = user.Items };
				Reduce = results =>
						 from result in results
						 group result by result.Country
							 into g
							 select new
							 {
								 Country = g.Key,
								 Count = g.Sum(x => x.Count)
							 };
			}
		}

		[Fact]
		public void MixedCaseReduce()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Country = "Israel" });
					session.Store(new User { Country = "ISRAEL" });
					session.Store(new User { Country = "israel" });
					session.SaveChanges();
				}

				new UsersByCountry().Execute(store);

				using (var session = store.OpenSession())
				{
					var r = session.Query<UsersByCountry.Result, UsersByCountry>()
						   .Customize(x => x.WaitForNonStaleResults())
						   .ToList();
					Assert.Equal(3, r.Count);
					Assert.Equal(1, r[0].Count);
					Assert.Equal(1, r[1].Count);
					Assert.Equal(1, r[2].Count);
				}
			}
		}

		[Fact]
		public void MixedCaseReduce_Complex()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Id = "orders/5057", Country = "ISRAEL", Items = 7 });
					session.Store(new User { Id = "orders/5046", Country = "ISRAEL", Items = 3 });
					session.Store(new User { Id = "orders/4627", Country = "israel", Items = 4 });

					session.SaveChanges();
				}

				new ItemssByCountry().Execute(store);

				using (var session = store.OpenSession())
				{
					var r = session.Query<ItemssByCountry.Result, ItemssByCountry>()
						   .Customize(x => x.WaitForNonStaleResults())
						   .ToList()
						   .OrderBy(x => x.Country)
						   .ToList();
					Assert.Equal(2, r.Count);
					Assert.Equal(4, r[0].Count);
					Assert.Equal(10, r[1].Count);
				}
			}
		}
	}
}