using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Json.Converters;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5014 : RavenTestBase
    {
        [Fact]
        public void can_delete_collection_except_few_docs()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 100; i++)
                        bulk.Store(new Company {Name = "name" + i});
                }

                WaitForIndexing(store);

                var toExclude = new [] { "companies/1-A", "companies/2-A", "companies/3-A" };

                using (var commands = store.Commands())
                {
                    var json = commands.RawDeleteJson<BlittableJsonReaderObject>("/studio/collections/docs?name=companies", new
                    {
                        ExcludeIds = toExclude
                    });

                    var operationId = JsonDeserializationClient.OperationIdResult(json);

                    new Operation(commands.RequestExecutor, () => store.Changes(), store.Conventions, operationId.OperationId).WaitForCompletion(TimeSpan.FromSeconds(15));

                    var collectionStats = store.Maintenance.Send(new GetCollectionStatisticsOperation());

                    Assert.Equal(3, collectionStats.Collections["Companies"]);
                }
            }
        }
    }
}