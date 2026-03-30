using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_22867 : RavenTestBase
    {
        public RavenDB_22867(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task RavenEtlShouldSkipUnsupportedIncrementalTimeSeriesAndContinue()
        {
            const string badDocId = "users/1";
            const string goodDocId = "users/2";
            const string tsName = Constants.Headers.IncrementalTimeSeriesPrefix + "HeartRate";
            var baseline = DateTime.UtcNow;

            var (src, dest, _) = Etl.CreateSrcDestAndAddEtl(collections: new[] { "Users" }, script: null);
            
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "bad" }, badDocId);
                session.IncrementalTimeSeriesFor(badDocId, tsName).Increment(baseline, 1);
                await session.SaveChangesAsync();
            }

            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "good" }, goodDocId);
                await session.SaveChangesAsync();
            }

            await Etl.WaitForEtlAsync(src, (n, s) =>
            {
                if (s.LoadErrors > 0)
                    throw new Exception($"Unexpected ETL load errors: {s.LoadErrors}");

                if (s.LastLoadErrorsInCurrentBatch.Errors.Count > 0)
                    throw new Exception($"Unexpected ETL batch load error details: {s.LastLoadErrorsInCurrentBatch}");

                if (s.WasLatestLoadSuccessful == false)
                    throw new Exception("Latest ETL load was not successful.");

                return s.LastProcessedEtag > 0;
            }, TimeSpan.FromSeconds(15));

            await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<User>(goodDocId);
            }, interval: 1000);
            var database = await Databases.GetDocumentDatabaseInstanceFor(src);

            Assert.True(WaitForValue(() => HasIncrementalTsEtlWarning(database), true));
        }


        private static bool HasIncrementalTsEtlWarning(DocumentDatabase db)
        {
            using (db.NotificationCenter.GetStored(out var notifications))
            {
                foreach (var notification in notifications)
                {
                    if (notification.Json.TryGet("Reason", out string reason) == false)
                        continue;

                    if (reason == AlertReason.Etl_Warning.ToString() &&
                        notification.Json.TryGet("Message", out string msg) &&
                        msg == "Incremental Time Series are not supported and are going to be skipped going forward. First encountered in document 'users/1' for time series 'INC:HeartRate'")
                        return true;
                }

                return false;
            }
        }
    }
}
