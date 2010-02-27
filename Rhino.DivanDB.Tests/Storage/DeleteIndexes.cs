using System;
using System.Linq;
using Microsoft.Isam.Esent.Interop;
using Xunit;

namespace Rhino.DivanDB.Tests.Storage
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
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
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
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
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