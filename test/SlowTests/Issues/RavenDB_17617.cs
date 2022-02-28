using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17617 : RavenLowLevelTestBase
{
    public RavenDB_17617(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Should_throw_on_attempt_to_create_auto_index()
    {
        using (var database = CreateDocumentDatabase())
        {
            using (var context = QueryOperationContext.ShortTermSingleUse(database))
            {
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                {
                    await database.QueryRunner.ExecuteQuery(new IndexQueryServerSide("from Users where LastName = 'Arek'")
                        {
                            DisableAutoIndexCreation = true
                        }, context, null,
                        OperationCancelToken.None);
                });

                Assert.Equal("Creation of Auto Indexes was disabled and no Auto Index matching the given query was found.", ex.Message);
            }
        }
    }
}
