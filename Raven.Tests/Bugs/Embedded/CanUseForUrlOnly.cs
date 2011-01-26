using Raven.Client.Client;
using Xunit;

namespace Raven.Tests.Bugs.Embedded
{
	public class CanUseForUrlOnly
	{
		[Fact]
		public void WontCreateDirectory()
		{
			var embeddableDocumentStore = new EmbeddableDocumentStore() 
			{
				Url = "http://localhost:8080"
			};
			embeddableDocumentStore.Initialize();
			Assert.Null(embeddableDocumentStore.DocumentDatabase);
		}
	}
}