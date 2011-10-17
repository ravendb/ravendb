using Raven.Client.Connection;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class EmbeddableDocumentStoreUsingUrlWithConnectionString
	{
		[Fact]
		public void ShouldWork()
		{
			using(var store = new EmbeddableDocumentStore
			{
				ConnectionStringName = "Server"
			})
			{
				store.Initialize();
				Assert.IsType<ServerClient>(store.DatabaseCommands);
				Assert.Null(store.DocumentDatabase);
			}
		}
	}
}