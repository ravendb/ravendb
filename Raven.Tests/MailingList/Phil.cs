using System;
using System.Collections.Generic;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Phil : RavenTest
	{
		public class Item
		{
			private string id;

			public string Id
			{
				get { return id; }
				set { id = value; }
			}

			public string Stuff
			{
				get { return stuff; }
				set { stuff = value; }
			}

			private string stuff;
			public string Summary;
			public string UserId;

			public Item()
			{

			}
			public Item(string id, string stuff)
			{
				this.id = id;
				this.stuff = stuff;
			}
		}
		public class UserItem
		{
			public string Name;
			public ItemReference Item;
		}
		public class User
		{
			public string FirstName, LastName;
			public List<UserItem> UserItems;
			public string Id;
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
							Item = new ItemReference {Id = item.Id, Summary = item.Summary, UserId = item.UserId},
							Name = "Stuff 2"
						}
					};
					s.SaveChanges();
				}

				using(var s = store.OpenSession())
				{
					var userLookup = s.Include<UserItem>(x => x.Item.Id).Load<User>(1);
					foreach (var uit in userLookup.UserItems)
					{
						var item2 = s.Load<Item>(uit.Item.Id);
					}
				}
			}
		}

		public class ItemReference
		{
			public string Id, Summary, UserId;
		}
	}
}