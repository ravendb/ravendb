using System;
using Xunit;

namespace Rhino.DivanDB.Tests.Storage
{
    public class CreateViews : AbstractDocumentStorageTest, IDisposable
    {
        private readonly DocumentDatabase db;

        public CreateViews()
        {
            db = new DocumentDatabase("divan.db.test.esent");
        }

        [Fact]
        public void Can_add_view_to_document()
        {
            db.AddView(
                @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
");
            var views = db.ViewStorage.ViewNames;
            Assert.Equal(1, views.Length);
            Assert.Equal("pagesByTitle", views[0]);
        }

        [Fact]
        public void Can_list_view_definition()
        {
            const string definition = @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
";
            db.AddView(
                definition);
            var actualDefinition = db.ViewStorage.GetViewDefinition("pagesByTitle");
            Assert.Equal(definition, actualDefinition);
        }

        public void Dispose()
        {
            db.Dispose();
        }
    }
}