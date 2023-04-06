using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Queries;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_20226 : RavenTestBase
{
    public RavenDB_20226(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void Can_Get_Index_Entries_For_Map_Reduce_Index_With_Decimal_Values(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var index = new Companies_StockPrices();
            index.Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Company { Name = "HR", Address = new Address { Country = "Israel" } }, "companies/1");
                session.Store(new Company { Name = "CF", Address = new Address { Country = "Poland" } }, "companies/2");

                var baseline = new DateTime(2023, 1, 1);

                for (var i = 0; i < 10; i++)
                {
                    var ts1 = session.TimeSeriesFor("companies/1", "StockPrices");

                    ts1.Append(baseline.AddDays(i), i);

                    var ts2 = session.TimeSeriesFor("companies/2", "StockPrices");

                    ts2.Append(baseline.AddDays(i), i * 5);
                }

                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            using (var commands = store.Commands())
            {
                var queryResult = commands.Query(new IndexQuery { Query = $"from index '{index.IndexName}' order by Country" }, indexEntriesOnly: true);
                var indexEntries = queryResult.Results;

                Assert.Equal(2, indexEntries.Length);

                var indexEntry = (BlittableJsonReaderObject)indexEntries[0];
                Assert.Equal("israel", indexEntry["Country"]);
                Assert.Equal("45.0", indexEntry["Volume"].ToString());

                indexEntry = (BlittableJsonReaderObject)indexEntries[1];
                Assert.Equal("poland", indexEntry["Country"]);
                Assert.Equal("225.0", indexEntry["Volume"].ToString());
            }
        }
    }

    private class Companies_StockPrices : AbstractTimeSeriesIndexCreationTask<Company, Companies_StockPrices.Result>
    {
        public class Result
        {
            public DateTime Date { get; set; }

            public string Country { get; set; }

            public decimal Volume { get; set; }
        }

        public Companies_StockPrices()
        {
            AddMap("StockPrices",
                segments => from segment in segments
                            let company = LoadDocument<Company>(segment.DocumentId)
                            from entry in segment.Entries
                            select new
                            {
                                Date = new DateTime(entry.Timestamp.Year, entry.Timestamp.Month, 1),
                                Country = company.Address.Country,
                                Volume = entry.Values[0]
                            });

            Reduce = results => from result in results
                                group result by new { result.Date, result.Country } into g
                                select new
                                {
                                    Date = g.Key.Date,
                                    Country = g.Key.Country,
                                    Volume = g.Sum(x => x.Volume)
                                };
        }
    }
}
