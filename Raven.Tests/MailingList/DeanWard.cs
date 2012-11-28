using System.ComponentModel.Composition.Hosting;
using Raven.Database.Plugins;
using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	using Raven.Abstractions.Indexing;
	using Raven.Client.Indexes;

	public class DeanWard : RavenTest
	{
		public class FilterEverything : AbstractReadTrigger
		{
			public override ReadVetoResult AllowRead(string key, Raven.Json.Linq.RavenJObject metadata, ReadOperation operation, Raven.Abstractions.Data.TransactionInformation transactionInformation)
			{
				if (operation == ReadOperation.Query)
					return ReadVetoResult.Ignore;
				return ReadVetoResult.Allowed;
			}
		}

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof(FilterEverything)));
		}

		[Fact]
		public void CanQueryWithAAndWildcard()
		{
			using (var store = this.NewDocumentStore())
			{
				new Users_Search().Execute(store);

				using (var sess = store.OpenSession())
				{
					sess.Query<User, Users_Search>().Where(x => x.Name.StartsWith("A")).ToList();
				}
			}
		}

		[Fact]
		public void CanQueryOnFilteredResultsUsingSmallPageSize()
		{
			using (var store = NewDocumentStore())
			{
				using (var sess = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						sess.Store(new User());
					}
					sess.SaveChanges();
				}

				using (var sess = store.OpenSession())
				{
					var user = sess.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.FirstOrDefault();

					Assert.Null(user);
				}
			}
		}

		public class Users_Search : AbstractIndexCreationTask<User>
		{
			public Users_Search()
			{
				this.Map = docs => from doc in docs
								   select new
								   {
									   doc.Name
								   };

				this.Index(x => x.Name, FieldIndexing.Analyzed);
			}
		}

		public class User
		{
			public string Name { get; set; }
		}
	}
}