using FastTests;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14450 : RavenLowLevelTestBase
    {
        public RavenDB_14450(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void MaxNumberOfDocsToDeleteParameterShouldBeRespected()
        {
            using (var db = CreateDocumentDatabase())
            {
                using (var ctx = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    using (var tx = ctx.OpenWriteTransaction())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            using (var doc = CreateDocument(ctx, $"users/{i}",
                                new DynamicJsonValue
                                {
                                    ["Name"] = "John", [Constants.Documents.Metadata.Key] = new DynamicJsonValue {[Constants.Documents.Metadata.Collection] = "Users"}
                                }))
                            {
                                db.DocumentsStorage.Put(ctx, $"users/{i}", null, doc);
                            }
                        }

                        tx.Commit();
                    }
                }

                using (var ctx = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    using (var tx = ctx.OpenWriteTransaction())
                    {
                        // should delete all
                        var result = db.DocumentsStorage.DeleteDocumentsStartingWith(ctx, "users/");
                        
                        Assert.Equal(10, result.Count);

                        // intentionally not committing
                    }

                    using (var tx = ctx.OpenWriteTransaction())
                    {
                        var result = db.DocumentsStorage.DeleteDocumentsStartingWith(ctx, "users/", maxDocsToDelete: 1);

                        Assert.Equal(1, result.Count);

                        result = db.DocumentsStorage.DeleteDocumentsStartingWith(ctx, "users/", maxDocsToDelete: 2);

                        Assert.Equal(2, result.Count);

                        result = db.DocumentsStorage.DeleteDocumentsStartingWith(ctx, "users/", maxDocsToDelete: 5);

                        Assert.Equal(5, result.Count);

                        result = db.DocumentsStorage.DeleteDocumentsStartingWith(ctx, "users/");

                        Assert.Equal(2, result.Count);
                    }
                }
            }
        }
    }
}
