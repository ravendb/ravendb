using System;
using System.Collections.Generic;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Phil : RavenTest
	{
		public class Item
		{

			public string Id { get; set; }

			public string Stuff { get; set; }

			public string Summary { get; set; }
			public string UserId { get; set; }

			public Item()
			{

			}
			public Item(string userId, string stuff)
			{
				UserId = userId;
				Stuff = stuff;
			}
		}
		public class UserItem
		{
			public string Name { get; set; }
			public ItemReference Item { get; set; }
		}

		public class User
		{
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public List<UserItem> UserItems { get; set; }
			public string Id { get; set; }
		}

		[Fact]
		public void CanUseInclude_Remote()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (var s = store.OpenSession())
				{
					var user = new User { FirstName = "Demo", LastName = "User" };
					s.Store(user);
					var item = new Item(user.Id, "Stuff");
					s.Store(item);

					user.UserItems = new List<UserItem>
					{
						new UserItem
						{
							Item = new ItemReference {Id = item.Id, Summary = item.Stuff, UserId = item.UserId},
							Name = "Stuff 2"
						}
					};
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var userLookup = s.Include<UserItem>(x => x.Item.Id).Load<User>(1);
					foreach (var uit in userLookup.UserItems)
					{
						s.Load<Item>(uit.Item.Id);
					}
				}
			}
		}

		public class ItemReference
		{
			public string Id { get; set; }
			public string Summary { get; set; }
			public string UserId { get; set; }
		}
	}
}