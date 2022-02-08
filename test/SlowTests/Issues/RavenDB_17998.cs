using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17998 : RavenTestBase
{
    public RavenDB_17998(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void Can_Compact_Index_With_NGram_Analyzer()
    {
        using (var store = GetDocumentStore(new Options
        {
            RunInMemory = false
        }))
        {
            var index = new Index_With_NGram_Analyzer();
            index.Execute(store);

            var operation = store.Maintenance.Server.Send(new CompactDatabaseOperation(new CompactSettings
            {
                Documents = false,
                DatabaseName = store.Database,
                Indexes = new[] { index.IndexName }
            }));

            operation.WaitForCompletion(TimeSpan.FromMinutes(1));
        }
    }

    private class Index_With_NGram_Analyzer : AbstractIndexCreationTask<Company>
    {
        public Index_With_NGram_Analyzer()
        {
            Map = companies => from c in companies
                               select new
                               {
                                   Name = c.Name
                               };

            Index(r => r.Name, FieldIndexing.Search);
            Analyzers.Add(n => n.Name, nameof(NGramAnalyzer));
        }
    }
}
