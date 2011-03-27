using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class ComplexUsage : LocalClientTest
	{
		[Fact]
		public void ShouldNotOutputNull()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Account
					{
						Name = "Hibernating Rhinos"
					});

					session.Store(new Design()
					{
						Name = "Raven",
						AccountId = "accounts/1"
					});
					session.Store(new User()
					{
						Name = null,
						AccountId = "accounts/1"
					});
					session.Store(new User()
					{
						Name = "Ayende",
						AccountId = "accounts/1"
					});
					session.SaveChanges();
				}

				new Accounts_Search().Execute(store);

				using(var session = store.OpenSession())
				{
					var objects = session.Query<dynamic,Accounts_Search>().Customize(x=>x.WaitForNonStaleResults()).ToArray();

				}
			}
		}

		public class Account
		{
			public string Name;
		}

		public class User
		{
			public string AccountId;
			public string Name;
		}

		public class Design
		{
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
    AccountName = g.Where(x=>x.AccountName != null).Select(x=>x.AccountName),
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