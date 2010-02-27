using System;
using Xunit;

namespace Rhino.DivanDB.Tests.Storage
{
    public class CreateIndexes : AbstractDocumentStorageTest, IDisposable
    {
        private readonly DocumentDatabase db;

        public CreateIndexes()
        {
            db = new DocumentDatabase("divan.db.test.esent");
        }

        [Fact]
        public void Index_with_same_name_can_be_added_twice()
        {
            db.PutIndex(
                @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
");

            db.PutIndex(
                @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
");
        }

        [Fact]
        public void Can_add_index()
        {
            db.PutIndex(
                @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
");
            var indexNames = db.IndexDefinitionStorage.IndexNames;
            Assert.Equal(1, indexNames.Length);
            Assert.Equal("pagesByTitle", indexNames[0]);
        }

        [Fact]
        public void Can_list_index_definition()
        {
            const string definition = @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
";
            db.PutIndex(definition);
            var actualDefinition = db.IndexDefinitionStorage.GetIndexDefinition("pagesByTitle");
            Assert.Equal(definition, actualDefinition);
        }

        public void Dispose()
        {
            db.Dispose();
        }
    }
}