using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class ravendb_failing_query_with_static_indexes_and_common_base_class : RavenTest
	{
		[Fact]
		public void it_fails_but_should_not()
		{
			using (var store = NewDocumentStore())
			{
				new Roots_ByUserId().Execute(store);	

				using (var session = store.OpenSession())
				{
					session.Store(new Root
					{
						User = new UserReference
						{
							Id = "Users/1"
						}
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var entities = session.Query<Root, Roots_ByUserId>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.User.Id == "Users/1")
						.ToArray();
					Assert.NotEmpty(entities);
				}
			}
		}

		public class Roots_ByUserId : AbstractIndexCreationTask<Root>
		{
			public Roots_ByUserId()
			{
				Map = cases => from e in cases
							   select new
							   {
								   User_Id = e.User.Id
							   };
			}
		}

		public class Root : Identifiable
		{
			public UserReference User { get; set; }
		}

		public class UserReference : Identifiable
		{
		}

		public class Identifiable
		{
			public string Id { get; set; }
		}
	}
}