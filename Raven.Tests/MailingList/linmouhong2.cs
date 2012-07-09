using System.Collections.Generic;
using System.Linq;
using Raven.Client.Linq;
using Raven.Tests;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class linmouhong2 : RavenTest
	{
		public class Advertiser
		{
			public int Id { get; set; }

			public IList<Shop> Shops { get; private set; }

			public Advertiser()
			{
				Shops = new List<Shop>();
			}
		}

		public class Shop
		{
			public string LoginPlatformId { get; set; }

			public string LoginAccountName { get; set; }

			public string Title { get; set; }
		}

		public class Microblogger
		{
			public int Id { get; set; }

			public string Nick { get; set; }

			public IList<Microblog> Microblogs { get; private set; }

			public Microblogger()
			{
				Microblogs = new List<Microblog>();
			}
		}

		public class Microblog
		{
			public string LoginPlatformId { get; set; }

			public string LoginAccountName { get; set; }

			public string Name { get; set; }

			public int FansCount { get; set; }

			public int TweetsCount { get; set; }
		}

		public enum UserType
		{
			Advertiser = 0,
			Microblogger = 1
		}

		public class LoginIndex : Raven.Client.Indexes.AbstractMultiMapIndexCreationTask<LoginIndexResult>
		{
			public LoginIndex()
			{
				AddMap<Advertiser>(users => from u in users
											from shop in u.Shops
											select new
											{
												LoginPlatformId = shop.LoginPlatformId,
												LoginAccountName = shop.LoginAccountName,
												UserId = u.Id,
												UserType = UserType.Advertiser
											});

				AddMap<Microblogger>(users => from u in users
											  from blog in u.Microblogs
											  select new
											  {
												  LoginPlatformId = blog.LoginPlatformId,
												  LoginAccountName = blog.LoginAccountName,
												  UserId = u.Id,
												  UserType = UserType.Microblogger
											  });

				Store(u => u.UserId, Raven.Abstractions.Indexing.FieldStorage.Yes);
				Store(u => u.UserType, Raven.Abstractions.Indexing.FieldStorage.Yes);
				Store(u => u.LoginPlatformId, Raven.Abstractions.Indexing.FieldStorage.Yes);
				Store(u => u.LoginAccountName, Raven.Abstractions.Indexing.FieldStorage.Yes);

				Index(u => u.LoginPlatformId, Raven.Abstractions.Indexing.FieldIndexing.NotAnalyzed);
				Index(u => u.LoginAccountName, Raven.Abstractions.Indexing.FieldIndexing.NotAnalyzed);
			}
		}

		public class LoginIndexResult
		{
			public string LoginPlatformId { get; set; }

			public string LoginAccountName { get; set; }

			public string UserId { get; set; }

			public UserType UserType { get; set; }
		}

		[Fact]
		public void CanProject()
		{
			using(var store = NewDocumentStore())
			{
				new LoginIndex().Execute(store);
				var advertiser1 = new Advertiser();
				advertiser1.Shops.Add(new Shop
				{
					Title = "Shop 1",
					LoginPlatformId = "Ebay",
					LoginAccountName = "account1"
				});

				using (var session = store.OpenSession())
				{
					session.Store(advertiser1);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					// if I use AsProjection<>, then there's an "Input string was not in correct format" exception.
					// if I do not use AsProjection<>, then there's an "Invalid cast from Advertiser to LoginIndexResult" exception.
					var login = session.Query<LoginIndexResult, LoginIndex>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.AsProjection<LoginIndexResult>()
						.FirstOrDefault(it => it.LoginPlatformId == "Ebay" && it.LoginAccountName == "account1");

					Assert.Equal(UserType.Advertiser, login.UserType);
				}
			}
		}
	}
}