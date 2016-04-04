using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicMapReduce : RavenTestBase
    {
        //TODO: Create base class for indexing tests with methods like
        //TODO: LowLevel_CreateDocumentDatabase
        [Fact]
        public async Task CanUseSimpleReduction()
        {
            using (var db = LowLevel_CreateDocumentDatabase())
            {
                var mri = AutoMapReduceIndex.CreateNew(1,
                    new AutoMapReduceIndexDefinition("test", new[] {"Users"}, new[]
                    {
                        new IndexField
                        {
                            Name = "Count",
                            MapReduceOperation = FieldMapReduceOperation.Count
                        }
                    }, new[]
                    {
                        new IndexField
                        {
                            Name = "Location"
                        },
                    }), db);

                using (mri)
                {
                    CreateUsers(db);

                    var stats = new IndexingBatchStats();
                    mri.DoIndexingWork(stats, CancellationToken.None);

                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                    {
                        var queryResult = await mri.Query(new  IndexQuery(), context, CancellationToken.None);
                        
                        Assert.Equal(1, queryResult.Results.Count);
                        var result = queryResult.Results[0].Data;

                        string location;
                        Assert.True(result.TryGet("Location", out location));
                        Assert.Equal("Poland", location);

                        // TODO arek - retrieve numeric values

                        string countString;
                        Assert.True(result.TryGet("Count", out countString));
                        Assert.Equal("2", countString);

                        string countRangeString;
                        Assert.True(result.TryGet("Count_Range", out countRangeString));
                        Assert.Equal("2", countRangeString);
                    }
                }
            }
        }

        private static void CreateUsers(DocumentDatabase db)
        {
            using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    using (var doc = context.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "Arek",
                        ["Location"] = "Poland",
                        [Constants.Metadata] = new DynamicJsonValue
                        {
                            [Constants.RavenEntityName] = "Users"
                        }
                    }, "users/1"))
                    {
                        db.DocumentsStorage.Put(context, "users/1", null, doc);
                    }

                    using (var doc = context.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "Pawel",
                        ["Location"] = "Poland",
                        [Constants.Metadata] = new DynamicJsonValue
                        {
                            [Constants.RavenEntityName] = "Users"
                        }
                    }, "users/2"))
                    {
                        db.DocumentsStorage.Put(context, "users/2", null, doc);
                    }

                    tx.Commit();
                }

                //LowLevel_WaitForIndexMap(mri, 2);
            }
        }
    }
}