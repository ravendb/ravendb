using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class WillRemoveTypesThatNotExistsOnTheServer : RavenTest
	{
		[Fact]
		public void CanQueryAStronglyTypedIndex()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User {Name = "Hibernating Rhinos", Address = new Address {City = "Hadera"}});
					session.SaveChanges();
				}

				new StronglyTypedIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					var results = session.Query<StronglyTypedIndex.Result, StronglyTypedIndex>()
						.Customize(customization => customization.WaitForNonStaleResults())
						.ToList();

					Assert.NotEmpty(results);
				}
			}
		}

		private class User
		{
			public string Name { get; set; }
			public Address Address { get; set; }
		}

		public class Address
		{
			public string City { get; set; }
		}

		public class StronglyTypedIndex : AbstractMultiMapIndexCreationTask<StronglyTypedIndex.Result>
		{
			public class Result
			{
				public string Name { get; set; }
				public Address Address { get; set; }
			}

			public StronglyTypedIndex()
			{
				AddMap<User>(users => users.Select(user => new Result
				{
					Name = user.Name,
					Address = user.Address,
				}));

				Reduce = results => from result in results
				                    group result by new {result.Name, result.Address}
				                    into g
				                    select new Result
				                    {
					                    Name = g.Key.Name,
					                    Address = new Address {City = g.Key.Address.City},
				                    };
			}
		}
	}
}