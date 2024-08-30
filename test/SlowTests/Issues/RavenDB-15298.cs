using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15298 : RavenTestBase
    {
        public RavenDB_15298(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Can_create_index_using_crete_field_on_dates(Options options)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new CreateSampleDataOperation());
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = "index",
                Maps = new HashSet<string>
                {
                    @"from e in docs.Employees
select new {
    _ = e.Select(x=> {
            if (x.Key == ""@metadata"")
                return null;

            if (x.Key == ""Address"")
                return x.Value.Select(y => CreateField(""Address_"" + y.Key, y.Value.ToString()));
            
            return CreateField(x.Key, x.Value);
        })
}"
                }
            }));

            Indexes.WaitForIndexing(store);

            WaitForUserToContinueTheTest(store);

            var statistics = store.Maintenance.Send(new GetStatisticsOperation());
            Assert.Equal(IndexState.Normal, statistics.Indexes.Single(x => x.Name == "index").State);

        }
    }
}
