using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicMapReduce : RavenTestBase
    {
        //TODO: Create base class for indexing tests with methods like
        //TODO: LowLevel_CreateDocumentDatabase
        [Fact(Skip = "Need to do a lot more here")]
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
                        var query = await mri.Query(new  IndexQuery(), context, CancellationToken.None);
                        Assert.Equal(1L, query.ResultSize);
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