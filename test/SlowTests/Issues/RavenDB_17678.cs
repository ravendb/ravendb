using System;
using System.Collections.Generic;
using System.Data.HashFunction.Core.Utilities;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using CsvHelper.TypeConversion;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17678 : RavenTestBase
{
    public RavenDB_17678(ITestOutputHelper output) : base(output)
    {
    }


    [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanExportToCsvTimeSeriesQuery(Options options)
    {
        var q1 = @"from 'Companies'
            where Address.Country ='USA'
            select timeseries(
                from 'StockPrices'
            between '2019-06-01T00:00:00.000Z'
            and '2020-02-01T00:00:00.000Z'
                )";

        var q2 = @"from 'Companies'
            where Address.Country ='USA'
            select timeseries(
                from 'StockPrices'
            between '2019-06-01T00:00:00.000Z'
            and '2020-02-01T00:00:00.000Z'
            select first(), avg()
                ) as t";

        using (var store = GetDocumentStore(options))
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.Documents | DatabaseItemType.TimeSeries));

            await AssertRawTimeSeriesQuery(store, q1);
            await AssertAggregatedTimeSeriesQuery(store, q2);
        }
    }

    private static async Task AssertAggregatedTimeSeriesQuery(DocumentStore store, string q2)
    {
        var httpClient = store.GetRequestExecutor().HttpClient;
        await using var stream = await httpClient.GetStreamAsync($"{store.Urls[0]}/databases/{store.Database}/streams/queries?query={q2}&format=csv");

        using var csvReader = new CsvReader(new StreamReader(stream), new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = ",", });

        var hasValues = false;
        await foreach (var r in csvReader.GetRecordsAsync<ExportedAggregatedTimeSeries>())
        {
            Assert.StartsWith("companies/", r.Id);
            Assert.True(r.From > DateTime.MinValue);
            Assert.True(r.To > DateTime.MinValue);
            Assert.Equal(5, r.Average.Length);
            Assert.Equal(5, r.Count.Length);
            Assert.Equal(5, r.First.Length);
            Assert.True(r.Average.Sum(Math.Abs) > 0);
            Assert.True(r.Count.Sum(Math.Abs) > 0);
            Assert.True(r.First.Sum(Math.Abs) > 0);
            hasValues = true;
        }
        Assert.True(hasValues);
    }

    private static async Task AssertRawTimeSeriesQuery(DocumentStore store, string q1)
    {
        var httpClient = store.GetRequestExecutor().HttpClient;
        await using var stream = await httpClient.GetStreamAsync($"{store.Urls[0]}/databases/{store.Database}/streams/queries?query={q1}&format=csv");

        using var csvReader = new CsvReader(new StreamReader(stream), new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = ",", });
        
        var hasValues = false;
        await foreach (var r in csvReader.GetRecordsAsync<ExportedRawTimeSeries>())
        {
            Assert.StartsWith("companies/", r.Id);
            Assert.StartsWith("employees/", r.Tag);
            Assert.True(r.Timestamp > DateTime.MinValue);
            Assert.Equal(5, r.Values.Length);
            Assert.True(r.Values.Sum(Math.Abs) > 0);
            Assert.False(r.IsRollup);
            hasValues = true;
        }
        Assert.True(hasValues);
    }

    private class ExportedAggregatedTimeSeries
    {
        [Name("@id")]
        public string Id { get; set; }
        public DateTime From { get; set; }
        public DateTime To { get; set; }
        [TypeConverter(typeof(TimeSeriesValuesConverter))]
        public double[] Count { get; set; }
        [TypeConverter(typeof(TimeSeriesValuesConverter))]
        public double[] First { get; set; }
        [TypeConverter(typeof(TimeSeriesValuesConverter))]
        public double[] Average { get; set; }
    }

    private class ExportedRawTimeSeries
    {
        [Name("@id")]
        public string Id { get; set; }
        public string Tag { get; set; }
        public DateTime Timestamp { get; set; }
        [TypeConverter(typeof(TimeSeriesValuesConverter))]
        public double[] Values { get; set; }
        public bool IsRollup { get; set; }
    }

    private class TimeSeriesValuesConverter : DefaultTypeConverter
    {
        public override object ConvertFromString(string text, IReaderRow row, MemberMapData memberMapData)
        {
            if (string.IsNullOrEmpty(text))
                return null;

            var result = new List<double>();
            var size = 0;
            Span<byte> buffer = stackalloc byte[64];
            var input = text.AsSpan();

            for (int i = 0; i < input.Length; i++)
            {
                var b = input[i];
                if (b == '[' || b == '"')
                    continue;

                if (b == ']')
                {
                    if (size > 0)
                    {
                        result.Add(double.Parse(buffer[..size]));
                    }
                }

                if (b == ',')
                {
                    result.Add(double.Parse(buffer[..size]));
                    size = 0;
                    continue;
                }

                buffer[size++] = (byte)b;
            }

            return result.ToArray();
        }

        public override string ConvertToString(object value, IWriterRow row, MemberMapData memberMapData) => throw new NotImplementedException();
    }
}
