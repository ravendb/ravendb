//-----------------------------------------------------------------------
// <copyright file="AccurateCount.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class AccurateCount : RavenTest
	{
		[Fact]
		public void QueryableCountIsAccurate()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Users",
				                                new IndexDefinition
				                                {
				                                	Map = "from user in docs.Users select new { user.Name }"
				                                });

				using(var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						var clone = new User{Name = "Ayende #"+i};
						s.Store(clone);
					}
					s.SaveChanges();
				}

				// wait for index
				using (var s = store.OpenSession())
				{
					var count = s.Query<User>("Users")
						.Customize(x=>x.WaitForNonStaleResults())
						.Count();
					Assert.Equal(5, count);
				}

				using (var s = store.OpenSession())
				{
					var queryable = s.Query<User>("Users");
					Assert.Equal(queryable.ToArray().Length, queryable.Count());
				}
			}
		}
		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}
