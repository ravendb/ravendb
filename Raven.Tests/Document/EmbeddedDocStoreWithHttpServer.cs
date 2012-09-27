using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Document
{
	public class EmbeddedDocStoreWithHttpServer : RavenTest
	{
		[Fact]
		public void WillInstructStudioToHideIt()
		{
			using(var store = new EmbeddableDocumentStore
			{
				UseEmbeddedHttpServer = true,
				Configuration =
				{
					Port = 8079
				},
				RunInMemory = true
			})
			{
				store.Initialize();
				WaitForUserToContinueTheTest();
				var value = store.DatabaseCommands.Get("Raven/StudioConfig").DataAsJson.Value<bool>("WarnWhenUsingSystemDatabase");
				Assert.False(value);
			}
		} 
	}
}