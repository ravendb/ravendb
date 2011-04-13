using Raven.Client.Client;
using Raven.Client.Embedded;
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

		[Fact]
		public void WontCreateDirectoryWhenSettingStorage()
		{
			var embeddableDocumentStore = new EmbeddableDocumentStore()
			{
				Configuration =
					{
						DefaultStorageTypeName = "munin"
					},
				Url = "http://localhost:8080"
			};
			embeddableDocumentStore.Initialize();
			Assert.Null(embeddableDocumentStore.DocumentDatabase);
		}
	}
}