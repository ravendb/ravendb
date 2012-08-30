//-----------------------------------------------------------------------
// <copyright file="LukeQuerying.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class LukeQuerying : RavenTest
	{
		[Fact]
		public void Can_query_on_not_equal()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new User
					{
						Name = "Ayende"
					});

					s.SaveChanges();
				}

				using(var s = store.OpenSession())
				{
					var users = s.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name != "Oren")
						.ToList();

					Assert.Equal(1, users.Count);
				}
			}
		}

		[Fact]
		public void Can_query_on_not_null()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User
					{
						Name = "Ayende"
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var users = s.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name != null)
						.ToList();

					Assert.Equal(1, users.Count);
				}
			}
		}

		[Fact]
		public void Can_query_on_collection_primitiv()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User
					{
						Name = "Ayende",
						Tags = new[]{"Hello", "World"}
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var users = s.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Tags.Any(y=>y == "Hello"))
						.ToList();

					Assert.Equal(1, users.Count);
				}
			}
		}
	}
}
