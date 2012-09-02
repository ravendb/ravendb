using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class UsingSortOptions : RavenTest
	{
		[Fact]
		public void CanCreateIndexWithSortOptionsOnStringVal()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from user in docs.Users select new { user.Name }",
					SortOptions = {{"Name", SortOptions.StringVal}}
				});
				var indexDefinition = store.DatabaseCommands.GetIndex("test");

				Assert.Equal(SortOptions.StringVal, indexDefinition.SortOptions["Name"]);
			}
		}
	}
}
