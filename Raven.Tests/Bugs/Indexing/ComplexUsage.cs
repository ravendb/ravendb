using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class ComplexUsage : RavenTest
	{
		[Fact]
		public void ShouldNotOutputNull()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Account
					{
						Id = "accounts/2",
						Name = null
					});
					session.Store(new Account
					{
						Id = "accounts/1",
						Name = "Hibernating Rhinos"
					});
					session.Store(new Design()
					{
						Id = "designs/1",
						Name = "Design 1",
						AccountId = "accounts/1"
					});
					session.Store(new User()
					{
						Id = "users/1",
						Name = null,
						AccountId = "accounts/1"
					});
					session.Store(new User()
					{
						Id = "users/2",
						Name = "User 1",
						AccountId = "accounts/1"
					});
					session.SaveChanges();
				}

				new Accounts_Search().Execute(store);

				using (var session = store.OpenSession())
				{
					var objects = session.Query<object, Accounts_Search>()
						.Customize(x => x.WaitForNonStaleResults())
						.AsProjection<AccountIndex>()
						.OrderBy(x => x.AccountId) //this is just to make sure the second result is last for the test
						.ToArray();


					Assert.Equal("Hibernating Rhinos", objects[0].AccountName);

					//Ayende, the account name for the second item
					//should be null but it's actually the string
					//NULL_VALUE.
					Assert.Null(objects[1].AccountName);
				}
			}
		}

		public class AccountIndex
		{
			public string AccountId;
			public string AccountName;
			public string[] UserName;
			public string[] DesignName;
		}

		public class Account
		{
			public string Id { get; set; }
			public string Name;
		}

		public class User
		{
			public string Id { get; set; }
			public string AccountId;
			public string Name;
		}

		public class Design
		{
			public string Id { get; set; }
			public string AccountId;
			public string Name;
		}

		public class Accounts_Search : AbstractIndexCreationTask<object, Account>
		{
			public override IndexDefinition CreateIndexDefinition()
			{
				var index = new IndexDefinition
				{
					Name = this.IndexName,
					Map =
						@"
		from doc in docs.WhereEntityIs(""Accounts"", ""Users"", ""Designs"")
		let acc = doc[""@metadata""][""Raven-Entity-Name""] == ""Accounts"" ? doc : null
		let user = doc[""@metadata""][""Raven-Entity-Name""] == ""Users"" ? doc : null
		let design = doc[""@metadata""][""Raven-Entity-Name""] == ""Designs"" ? doc : null
		select new 
		{
		    AccountId = acc != null ? acc.Id : (user != null ? user.AccountId : design.AccountId),
		    AccountName = acc != null ? acc.Name : null,
		    UserName = user != null ? user.Name : null,
		    DesignName = design != null ? design.Name : null
		}",
					Reduce =
						@"
		from result in results 
		group result by result.AccountId into g
		select new 
		{
		    AccountId = g.Key,
		    AccountName = g.Where(x=>x.AccountName != null).Select(x=>x.AccountName).FirstOrDefault(),
		    UserName = g.Where(x=>x.UserName != null).Select(x=>x.UserName),
		    DesignName = g.Where(x=>x.DesignName != null).Select(x=>x.DesignName),
		}"
				};
				index.Indexes["AccountId"] = FieldIndexing.NotAnalyzed;
				index.Indexes["AccountName"] = FieldIndexing.Analyzed;
				index.Indexes["DesignName"] = FieldIndexing.Analyzed;
				index.Indexes["UserName"] = FieldIndexing.Analyzed;
				return index;
			}
		}

	}
}
