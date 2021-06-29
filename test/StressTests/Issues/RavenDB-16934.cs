using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16934 : RavenTestBase
    {
        public RavenDB_16934(ITestOutputHelper output) : base(output)
        {
        }

        [Fact64Bit]
        public async Task Should_Delete_All_Documents_Without_Timeout()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Databases.QueryOperationTimeout)] = "5";
                }
            }))
            {
                var now = DateTime.UtcNow;
                var toSave = now.AddHours(-2);

                new SearchIndex().Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 1_000_000; i++)
                    {
                        bulk.Store(new Unit
                        {
                            DateTime = toSave
                        });
                    }
                }

                WaitForIndexing(store);

                var operation = await store.Operations.SendAsync(
                    new DeleteByQueryOperation<SearchIndex.Result, SearchIndex>(x =>
                        x.DateTime < now));

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
            }
        }

        private class Unit
        {
            public DateTime DateTime { get; set; }
        }

        private class SearchIndex : AbstractIndexCreationTask<Unit>
        {
            public class Result
            {
                public DateTime DateTime { get; set; }
            }

            public SearchIndex()
            {
                Map = units => from unit in units
                    select new Result
                    {
                        DateTime = unit.DateTime
                    };
            }
        }
    }
}
