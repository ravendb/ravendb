using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6989 : RavenLowLevelTestBase
    {
        [Fact]
        public void multi_map_index_with_load_document_to_same_collection()
        {
            using (var database = CreateDocumentDatabase())
            using (var index = MapIndex.CreateNew(new IndexDefinition
            {
                Name = "Index",
                Maps =
                {
                    @"from user in docs.Users select new { Name = LoadDocument(""shippers/1"", ""Shippers"").Name }",
                    @"from product in docs.Products select new { Name = LoadDocument(""shippers/1"", ""Shippers"").Name }"
                },
                Type = IndexType.Map
            }, database))
            using (var contextPool = new TransactionContextPool(database.DocumentsStorage.Environment))
            {
                new IndexStorage(index, contextPool, database);
            }
        }
    }
}
