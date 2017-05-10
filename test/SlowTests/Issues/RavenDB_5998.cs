using System.IO.Compression;
using System.Linq;
using System.Reflection;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Xunit;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace SlowTests.Issues
{
    public class RavenDB_5998 : RavenLowLevelTestBase
    {
        [Theory]
        [InlineData("SlowTests.Smuggler.Northwind_3.5.35168.ravendbdump")]
        public void CanImportNorthwind(string file)
        {
            using (var inputStream = GetType().GetTypeInfo().Assembly.GetManifestResourceStream(file))
            using (var stream = new GZipStream(inputStream, CompressionMode.Decompress))
            {
                Assert.NotNull(stream);

                DocumentsOperationContext context;
                using (var database = CreateDocumentDatabase())
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {
                    var source = new StreamSource(stream, context, database);
                    var destination = new DatabaseDestination(database);

                    var smuggler = new DatabaseSmuggler(source, destination, database.Time, new DatabaseSmugglerOptions
                    {
                        TransformScript = "function(doc) { doc['Test'] = 'NewValue'; return doc; }"
                    });

                    var result = smuggler.Execute();

                    Assert.Equal(1059, result.Documents.ReadCount);
                    Assert.Equal(0, result.Documents.SkippedCount);
                    Assert.Equal(0, result.Documents.ErroredCount);

                    Assert.Equal(4, result.Indexes.ReadCount);
                    Assert.Equal(0, result.Indexes.ErroredCount);

                    Assert.Equal(1, result.Transformers.ReadCount);
                    Assert.Equal(0, result.Transformers.ErroredCount);

                    Assert.Equal(1, result.Identities.ReadCount);
                    Assert.Equal(0, result.Identities.ErroredCount);

                    Assert.Equal(0, result.RevisionDocuments.ReadCount);
                    Assert.Equal(0, result.RevisionDocuments.ErroredCount);

                    using (context.OpenReadTransaction())
                    {
                        var countOfDocuments = database.DocumentsStorage.GetNumberOfDocuments(context);
                        var countOfIndexes = database.IndexStore.GetIndexes().Count();
                        var countOfTransformers = database.TransformerStore.GetTransformers().Count();

                        Assert.Equal(1059, countOfDocuments);
                        Assert.Equal(3, countOfIndexes);// there are 4 in ravendbdump, but Raven/DocumentsByEntityName is skipped
                        Assert.Equal(1, countOfTransformers);

                        var doc = database.DocumentsStorage.Get(context, "orders/1");
                        string test;
                        Assert.True(doc.Data.TryGet("Test", out test));
                        Assert.Equal("NewValue", test);
                    }
                }
            }
        }
    }
}