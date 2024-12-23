using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Server.Config;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21247 : RavenTestBase
{
    public RavenDB_21247(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.None)]
    public async Task TestAddHintMethod()
    {
        DoNotReuseServer();

        using (var store = GetDocumentStore())
        {
            var db = await GetDatabase(store.Database);

            db.NotificationCenter
                .RequestLatency
                .AddHint(5, "Query", "some query");

            db.NotificationCenter
                .RequestLatency
                .UpdateRequestLatency(null);

            var storedRequestLatencyDetails = db.NotificationCenter.RequestLatency
                .GetRequestLatencyDetails();

            Assert.Equal(1, storedRequestLatencyDetails.RequestLatencies.Count);

            storedRequestLatencyDetails.RequestLatencies["Query"].TryDequeue(out RequestLatencyInfo result);

            Assert.Equal("some query", result.Query);
        }
    }
}
