using FastTests;
using Raven.Client;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12034 : RavenTestBase
    {
        [Fact]
        public void Stored_object_should_not_get_collection_metadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    object marker = new object();

                    session.Store(marker, "markers/1");

                    var metadata = session.Advanced.GetMetadataFor(marker);

                    Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Collection));
                }
            }
        }
    }
}
