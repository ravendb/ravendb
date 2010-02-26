using System;
using System.Linq;
using Microsoft.Isam.Esent.Interop;
using Xunit;

namespace Rhino.DivanDB.Tests.Storage
{
    public class DeleteViews : AbstractDocumentStorageTest, IDisposable
    {
        private readonly DocumentDatabase db;

        public DeleteViews()
        {
            db = new DocumentDatabase("divan.db.test.esent");
        }

        [Fact]
        public void Can_remove_view_to_document()
        {
            db.PutView(
                @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
");
            db.DeleteView("pagesByTitle");
            var views = db.ViewStorage.ViewNames;
            Assert.Equal(0, views.Length);
        }

        [Fact]
        public void Removing_view_will_remove_index()
        {
            const string definition = @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
";
            db.PutView(definition);
            db.DeleteView("pagesByTitle");
            var actualDefinition = db.IndexStorage.Indexes;
            Assert.Empty(actualDefinition);
        }

        public void Dispose()
        {
            db.Dispose();
        }
    }
}