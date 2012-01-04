using Raven.Client.Document;
using Xunit;
using Raven.Client.Extensions;

namespace Raven.Tests.MailingList
{
	public class BAM : LocalClientTest
	{
		[Fact]
		public void get_dbnames_test()
		{
			using (var server = GetNewServer())
			using (var docStore = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				var dbNames = docStore.DatabaseCommands.GetDatabaseNames();

				Assert.Empty(dbNames);

				docStore.DatabaseCommands.EnsureDatabaseExists("test");

				dbNames = docStore.DatabaseCommands.GetDatabaseNames();

				Assert.NotEmpty(dbNames);

			}
		}
	}
}
