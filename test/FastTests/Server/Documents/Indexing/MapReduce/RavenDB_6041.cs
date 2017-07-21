using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_6041 : RavenLowLevelTestBase
    {
        [Theory]
        [InlineData(1)]
        [InlineData(128)]
        public async Task Reduction_should_ignore_overflow_pages(long numberOfDocs)
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapReduceIndex.CreateNew(new IndexDefinition()
                {
                    Etag = 10,
                    Name = "Users_ByLocation",
                    Maps = { @"from user in docs.Users
select new { Location = user.Location, Count = 1 }" },
                    Reduce = @"from result in results
group result by result.Location into g
select new
{
    Location = g.Key,
    Count = g.Sum(x=> x.Count)
}",
                }, database))
                {
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        var bytes = new byte[4096];
                        new Random(2).NextBytes(bytes); // TODO arek - seed

                        var randomLocation = Encoding.ASCII.GetString(bytes);

                        using (var tx = context.OpenWriteTransaction())
                        {
                            for (int i = 0; i < numberOfDocs; i++)
                            {
                                var user = new DynamicJsonValue()
                                {
                                    ["Location"] = randomLocation,
                                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                    {
                                        [Constants.Documents.Metadata.Collection] = "Users"
                                    }
                                };

                                using (var doc = CreateDocument(context, $"users/{i}", user))
                                {
                                    database.DocumentsStorage.Put(context, $"users/{i}", null, doc);
                                }
                            }

                            tx.Commit();
                        }

                        var firstRunStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(firstRunStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        Assert.Equal(numberOfDocs, firstRunStats.MapAttempts);
                        Assert.Equal(numberOfDocs, firstRunStats.MapSuccesses);
                        Assert.Equal(0, firstRunStats.MapErrors);

                        var queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                        Assert.False(queryResult.IsStale);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal(numberOfDocs, queryResult.Results[0].Data["Count"]);
                    }
                }
            }
        }
    }
}