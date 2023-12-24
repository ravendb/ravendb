using System;
using System.Threading.Tasks;
using SlowTests.Server.Documents.ETL;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21866 : EtlTestBase
    {
        public RavenDB_21866(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.TimeSeries)]
        public async Task EtlCanLoadTimeSeriesByComparingTheNameInItsOriginalCasing()
        {
            const string collection = "EventDocuments";
            const string id = $"{collection}/1";
            const string tsName = "AppOpen";
            const string script = @"
loadToEventDocuments(this);

function loadCountersOfEventDocumentsBehavior(documentId, counterName) {
   return true;
}

function loadTimeSeriesOfEventDocumentsBehavior(docId, timeSeriesName) {
   if (timeSeriesName =='AppOpen'){
       return true;
   }
   return false
}";

            var (src, dest, _) = CreateSrcDestAndAddEtl(new[] { collection }, script);

            var etlDone = WaitForEtl(src, (s, statistics) => statistics.LoadSuccesses > 0);

            using (var session = src.OpenAsyncSession())
            {
                var date = DateTime.Today.ToUniversalTime();
                var doc = new EventDocument { Name = "event", Date = date };
                await session.StoreAsync(doc, id);

                var tsFor = session.TimeSeriesFor(doc, tsName);
                for (int i = 0; i < 10; i++)
                {
                    tsFor.Append(date.AddMinutes(i), i);
                }

                await session.SaveChangesAsync();
            }

            Assert.True(etlDone.Wait(TimeSpan.FromSeconds(15)));

            using (var session = dest.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<EventDocument>(id);
                Assert.NotNull(doc);

                var entries = await session.TimeSeriesFor(id, tsName).GetAsync();
                Assert.Equal(10, entries?.Length);
            }

        }

        private class EventDocument
        {
            public DateTime Date { get; set; }

            public string Name { get; set;  }

        }
    }
}
