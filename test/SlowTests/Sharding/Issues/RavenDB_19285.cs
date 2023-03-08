using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Queries;
using SlowTests.Client.TimeSeries.Query;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_19285: RavenTestBase
{
    public RavenDB_19285(ITestOutputHelper output) : base(output)
    {
    }


    [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task TimeSeriesQueryResultShouldAddTimeSeriesFields(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var baseline = RavenTestHelper.UtcToday;

            using (var session = store.OpenSession())
            {
                session.Store(new TimeSeriesLinqQuery.Person
                {
                    Name = "ayende"
                }, "people/1");

                var tsf = session.TimeSeriesFor("people/1", "Heartrate");
                tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                tsf = session.TimeSeriesFor("people/1", "BloodPressure");
                tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var cmd = new RawTimeSeriesQueryCommandWithTimeSeriesFields(new IndexQuery()
                {
                    Query =
                        "from 'People' as p select id() as Id, Name, timeseries(from p.Heartrate where (Tag == 'watches/fitbit')) as HeartRate, timeseries(from p.BloodPressure where (Tag == 'watches/apple')) as BloodPressure limit 0, 1"
                });

                await commands.ExecuteAsync(cmd);

                var result = cmd.Result;

                Assert.True(result.TryGet<BlittableJsonReaderArray>(nameof(DocumentQueryResult.TimeSeriesFields), out var tsFieldNames));

                var names = tsFieldNames.Select(x => x.ToString()).ToList();

                Assert.Contains("HeartRate", names);
                Assert.Contains("BloodPressure", names);
            }
        }
    }

    private class RawTimeSeriesQueryCommandWithTimeSeriesFields : RavenCommand<BlittableJsonReaderObject>
    {
        private readonly IndexQuery _indexQuery;
        public override bool IsReadRequest => true;

        public override RavenCommandResponseType ResponseType { get; protected internal set; } = RavenCommandResponseType.Raw;

        public RawTimeSeriesQueryCommandWithTimeSeriesFields(IndexQuery indexQuery)
        {
            _indexQuery = indexQuery;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var path = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/queries?queryHash=1234");

            path.Append("&addTimeSeriesNames=true");
            
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteIndexQuery(DocumentConventions.Default, ctx, _indexQuery);
                        }
                    }, DocumentConventions.Default
                )
            };

            url = path.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = response;
        }
    }
}
