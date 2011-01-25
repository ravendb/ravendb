using System.Linq;
using Raven.Tests.Queries;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SelectManyOnNull : LocalClientTest
	{
		[Fact]
		public void ShouldNotThrow()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User());
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Advanced.LuceneQuery<User>()
						.WhereEquals("Tags,Id", "1")
						.ToArray();
				}

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}

		public class User
		{
			public Tag[] Tags { get; set; }
		}

		public class Tag
		{
			
		}
	}
}