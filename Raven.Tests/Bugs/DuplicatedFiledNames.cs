using Raven.Abstractions.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class DuplicatedFiledNames : RavenTest
	{
		[Fact]
		public void ShouldNotDoThat()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test",
				                                new IndexDefinition
				                                {
				                                	Map = "from doc in docs.ClickBalances select new { doc.AccountId } "
				                                });

				using(var s = store.OpenSession())
				{
					var accountId = 1;
					s.Query<ClickBalance>("test")
						.Where(x => x.AccountId == accountId)
						.ToList();
				}
			}
		}
	}

	public class ClickBalance
	{
		public int AccountId { get; set; }
	}
}