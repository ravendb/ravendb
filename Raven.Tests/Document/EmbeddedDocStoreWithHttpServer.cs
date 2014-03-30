using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Document
{
    public class EmbeddedDocStoreWithHttpServer : RavenTest
    {
        protected override void ModifyStore(EmbeddableDocumentStore documentStore)
        {
            documentStore.UseEmbeddedHttpServer = true;
        }

        [Fact]
        public void WillInstructStudioToHideIt()
        {
            using (var store = NewDocumentStore(runInMemory: true, port: 8079))
            {
                var value = store.DatabaseCommands.Get("Raven/StudioConfig").DataAsJson.Value<bool>("WarnWhenUsingSystemDatabase");
                Assert.False(value);
            }
        }
    }
}