using System;
using Rhino.DivanDB.Linq;
using Xunit;

namespace Rhino.DivanDB.Tests.Storage
{
    public class CreateAndDeleteViews : AbstractDocumentStorageTest, IDisposable
    {
        private readonly DocumentDatabase db;

        public CreateAndDeleteViews()
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
            var views = db.ListView();
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
            var actualDefinition = db.ViewDefinitionByName("pagesByTitle");
            Assert.Equal(definition, actualDefinition);
        }

        [Fact]
        public void Can_get_view_instance_by_name()
        {
            const string definition = @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
";
            db.AddView(
                definition);
            var actualDefinition = db.ViewInstanceByName("pagesByTitle");
            Assert.IsAssignableFrom<ViewFunc>(actualDefinition);
        }

        [Fact]
        public void View_instances_will_be_cached()
        {
            const string definition = @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
";
            db.AddView(
                definition);
            var actualDefinition1 = db.ViewInstanceByName("pagesByTitle");
            var actualDefinition2 = db.ViewInstanceByName("pagesByTitle");
            Assert.Same(actualDefinition1, actualDefinition2);
        }

        public void Dispose()
        {
            db.Dispose();
        }
    }
}