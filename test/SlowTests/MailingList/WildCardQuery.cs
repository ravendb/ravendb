using FastTests;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.MailingList
{
    public class WildCardQuery : RavenTestBase
    {
        [Fact]
        public void CanQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Query("dynamic", new IndexQuery()
                    {
                        Query = "PortalId:0 AND Query:(*) QueryBoosted:(*)"
                    });
                }
            }
        }
    }
}
