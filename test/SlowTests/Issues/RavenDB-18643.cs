using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18643 : RavenTestBase
{
    public RavenDB_18643(ITestOutputHelper output) : base(output)
    {
    }
    
    private record ExampleItem(string Name);

    [Fact]
    public async Task CanGetProgressOfBulkInsert()
    {
        List<string> lastInsertedDocId = new();
        using var store = GetDocumentStore();
        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMinutes(3));
        using (var bulkInsert = store.BulkInsert(token: cts.Token))
        {
            bulkInsert.OnProgress += (sender, args) =>
            {
                lastInsertedDocId.Add(args.Progress.LastProcessedId);
                Assert.NotEmpty(args.Progress.LastProcessedId);
            };

            var i = 0;

            while (cts.IsCancellationRequested || lastInsertedDocId.Count == 0)
            {
                await bulkInsert.StoreAsync(new ExampleItem($"ExampleItem/{i++}"));
                await Task.Delay(TimeSpan.FromMilliseconds(200));
            }
        }

        Assert.NotEmpty(lastInsertedDocId);
    }
}
