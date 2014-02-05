using Raven.Client.Connection;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class EmbeddableDocumentStoreUsingUrlWithConnectionString : RavenTest
	{
		[Fact]
		public void ShouldWork()
		{
			using(var store = NewDocumentStore())
			{
				Assert.IsType<ServerClient>(store.DatabaseCommands);
				Assert.Null(store.DocumentDatabase);
			}
		}
	}
}