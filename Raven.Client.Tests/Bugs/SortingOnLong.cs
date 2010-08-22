using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Bugs
{
	public class SortingOnLong : LocalClientTest
	{
		[Fact]
		public void CanSortOnLong()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Foo
					{
						Value = 30000000000
					});

					session.Store(new Foo
					{
						Value = 25
					});

					session.Store(new Foo
					{
						Value = 3147483647
					});

					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("long",
				                                new IndexDefinition
				                                {
				                                	Map = "from doc in docs select new { doc.Value }",
													SortOptions = {{"Value", SortOptions.Long}}
				                                });

				using (var session = store.OpenSession())
				{
					var foos = session.LuceneQuery<Foo>("long")
						.WaitForNonStaleResults()
						.OrderBy("Value_Range")
						.ToList();

					Assert.Equal(3, foos.Count);

					Assert.Equal(25, foos[0].Value);
					Assert.Equal(3147483647, foos[1].Value);
					Assert.Equal(30000000000, foos[2].Value);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public long Value { get; set; }

			public override string ToString()
			{
				return string.Format("Id: {0}, Value: {1}", Id, Value);
			}
		}
	}
}