using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13278 : RavenTestBase
    {
        public RavenDB_13278(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_not_allow_having_two_revision_collection_configurations_with_collection_name_that_differs_only_in_casing()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        {
                            "Users", new RevisionsCollectionConfiguration
                            {
                                PurgeOnDelete = true

                            }
                        },
                        {
                            "users", new RevisionsCollectionConfiguration
                            {
                                MinimumRevisionsToKeep = 5
                            }
                        }
                    }
                };

                var ex = Assert.Throws<RavenException>( () => 
                    store.Maintenance.Send(new ConfigureRevisionsOperation(configuration)));


                Assert.Contains("Cannot have two different revision configurations on the same collection", ex.Message);

            }

        }
    }
}
