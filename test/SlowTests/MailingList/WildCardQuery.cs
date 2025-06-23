using FastTests;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class WildCardQuery : RavenTestBase
    {
        public WildCardQuery(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Query(new IndexQuery
                    {
                        Query = "FROM @all_docs WHERE PortalId = 0 AND search(Query, '*') OR search(QueryBoosted, '*')"
                    });
                }
            }
        }
    }
}
