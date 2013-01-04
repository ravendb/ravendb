using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class FindPropertyNameForIndex : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
		}

		[Fact]
		public void ShouldNotOutputNull()
		{
			using (var store = NewDocumentStore())
			{

				using (var session = store.OpenSession())
				{
					session.Store(new Customer
					{
						Id = "accounts/2",
						Name = "ACME Anvils",
						Number = "1234",
						Dealer = new AccountReference { Id = "accounts/7" }
					});
					session.Store(new Customer
					{
						Id = "accounts/1",
						Name = "Hibernating Rhinos",
						Number = "98765",
						Dealer = new AccountReference { Id = "accounts/6" }
					});
					session.SaveChanges();
				}

				new Accounts_FindPropertyNameForIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					var objects = session.Query<Customer, Accounts_FindPropertyNameForIndex>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Dealer.Id == "accounts/6")
						.OrderBy(x => x.Id)
						.ToArray();

					Assert.NotEmpty(objects);
				}
			}
		}

		public class AccountReference
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Customer
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Number { get; set; }
			public AccountReference Dealer { get; set; }
		}
		 public class Accounts_FindPropertyNameForIndex : AbstractIndexCreationTask<Customer>
		{
			public Accounts_FindPropertyNameForIndex()
			{
				Map = docs => docs.Select(x => new { Dealer_Id = x.Dealer.Id });
			}
		}
	}
}
