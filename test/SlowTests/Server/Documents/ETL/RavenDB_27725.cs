using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_27725 : RavenTestBase
    {
        public RavenDB_27725(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task Documents_failing_transformation_respect_batch_limits_and_advance_last_processed_etag()
        {
            const int batchLimit = 10;
            const int numberOfDocs = 50;

            using (var src = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Etl.MaxNumberOfExtractedDocuments)] = batchLimit.ToString()
            }))
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenAsyncSession())
                {
                    for (var i = 0; i < numberOfDocs; i++)
                        await session.StoreAsync(new User { Name = "Joe Doe" }, $"users/{i}");

                    await session.SaveChangesAsync();
                }

                var lastDocEtag = (await src.Maintenance.SendAsync(new GetStatisticsOperation())).LastDocEtag;
                Assert.NotNull(lastDocEtag);

                Etl.AddEtl(src, dest, "Users", script: "throw new Error('failing transformation'); loadToUsers(this);");

                var database = await GetDatabase(src.Database);
                var etlProcess = (RavenEtl)database.EtlLoader.Processes.First();

                Assert.Equal(lastDocEtag.Value, await WaitForValueAsync(() => etlProcess.Statistics.LastProcessedEtag, lastDocEtag.Value, timeout: 60_000));

                var stats = etlProcess.GetPerformanceStats();

                Assert.Equal(numberOfDocs, stats.Sum(x => x.TransformationErrorCount));
                Assert.Null(etlProcess.Statistics.LastSuccessfulBatchTime);
                Assert.True(stats.Max(x => x.NumberOfExtractedItems[EtlItemType.Document]) <= batchLimit);

                Assert.Equal(0, (await dest.Maintenance.SendAsync(new GetStatisticsOperation())).CountOfDocuments);
            }
        }
    }
}
