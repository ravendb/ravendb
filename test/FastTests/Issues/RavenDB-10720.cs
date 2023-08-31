using Tests.Infrastructure;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_10720 : RavenTestBase
    {
        public RavenDB_10720(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanSaveDocumentWithMetadata(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var entity = new User();
                    session.Store(entity);
                    var metadata = session.Advanced.GetMetadataFor(entity);
                    metadata.Add("property--length-19", true);
                    session.SaveChanges();
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
        }
    }
}
