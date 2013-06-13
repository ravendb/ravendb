using System.Threading.Tasks;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Core
{
	public class AsyncDatabaseCommandsTests : RavenTestBase
	{
		[Fact]
		public async Task CanGetAListOfDatabasesAsync()
		{
			var dbname = GenerateNewDatabaseName();
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				var names = await store.AsyncDatabaseCommands.GetDatabaseNamesAsync(25);
				Assert.Contains(dbname, names);
			}
		}
	}
}