using System;
using Raven.Database;
using Xunit;

namespace Raven.Tests.Storage
{
    public class DeleteIndexes : AbstractDocumentStorageTest, IDisposable
    {
        private readonly DocumentDatabase db;

        public DeleteIndexes()
        {
            db = new DocumentDatabase("divan.db.test.esent");
        }

        [Fact]
        public void Can_remove_index()
        {
            db.PutIndex("pagesByTitle",
                        @"
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = doc.size };
");
            db.DeleteIndex("pagesByTitle");
            var views = db.IndexDefinitionStorage.IndexNames;
            Assert.Equal(0, views.Length);
        }

        [Fact]
        public void Removing_index_remove_it_from_index_storage()
        {
            const string definition = @"
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = doc.size };
";
            db.PutIndex("pagesByTitle", definition);
            db.DeleteIndex("pagesByTitle");
            var actualDefinition = db.IndexStorage.Indexes;
            Assert.Empty(actualDefinition);
        }

        public void Dispose()
        {
            db.Dispose();
        }
    }
}