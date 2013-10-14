using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Core
{
	public class Indexes : RavenTestBase
	{
		[Fact]
		public async Task CanGetASingleIndexByName()
		{
			var dbname = GenerateNewDatabaseName();
			using (var documentStore = NewDocumentStore())
			{
				await documentStore.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(dbname);

				await documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.PutIndexAsync("Test", new IndexDefinition
					{
						Map = "from doc in docs.Companies select new { doc.Name }"
					}, true);

				var result = await documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetIndexAsync("Test");
				Assert.Equal("Test", result.Name);
			}
		}
	}
}