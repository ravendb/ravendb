using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Bugs.Embedded
{
	public class CanUseForUrlOnly
	{
		[Fact]
		public void WontCreateDirectory()
		{
			using (var embeddableDocumentStore = new EmbeddableDocumentStore()
			{
				Url = "http://localhost:8079"
			})
			{
				embeddableDocumentStore.Initialize();
				Assert.Null(embeddableDocumentStore.DocumentDatabase);
			}
		}

		[Fact]
		public void WontCreateDirectoryWhenSettingStorage()
		{
			using (var embeddableDocumentStore = new EmbeddableDocumentStore()
			{
				Configuration =
					{
						DefaultStorageTypeName = "munin"
					},
				Url = "http://localhost:8079"
			})
			{
				embeddableDocumentStore.Initialize();
				Assert.Null(embeddableDocumentStore.DocumentDatabase);
			}
		}
	}
}