using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues;

public class RavenDB_19149 : RavenTestBase
{
    public RavenDB_19149(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void LuceneOptimizeIsNotHanging()
    {
        using var store = GetDocumentStore(new Options()
        {
            RunInMemory = false,
            ModifyDatabaseRecord = record =>
            {
                //This should stop all merges.
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MaxTimeForMergesToKeepRunning)] = "0";
            }
        });

        PrepareDataOnTheServer(store, out ExampleIndex index);

        var settings = new CompactSettings
        {
            DatabaseName = store.Database,
            Documents = true,
            Indexes = new[] { index.IndexName }
        };

        var operation = store.Maintenance.Server.Send(new CompactDatabaseOperation(settings));

        operation.WaitForCompletion(TimeSpan.FromMinutes(1));
    }

    private void PrepareDataOnTheServer(DocumentStore store, out ExampleIndex exampleIndex)
    {
        {
            using var bulkInsert = store.BulkInsert();
            var random = new Random(1241231);
            var names = Enumerable.Range(0, 10).Select(i => $"Name{i}").ToArray();
            for (int i = 0; i < 100; ++i)
            {
                bulkInsert.Store(new Test(names[random.Next(names.Length)], names[random.Next(names.Length)]));
            }
        }
        exampleIndex = new ExampleIndex();
        exampleIndex.Execute(store);
        Indexes.WaitForIndexing(store);
    }

    private record Test(string Name, string LastName);
    private class ExampleIndex : AbstractIndexCreationTask<Test>
    {
        public ExampleIndex()
        {
            Map = tests => tests.Select(i => new {Name = i.Name, LastName = i.LastName});
        }
    }
    
}
