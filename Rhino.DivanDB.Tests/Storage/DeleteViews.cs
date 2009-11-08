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
            db.AddView(
                @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
");
            db.DeleteView("pagesByTitle");
            var views = db.ListView();
            Assert.Equal(0, views.Length);
        }

        [Fact]
        public void Removing_view_will_remove_view_definition()
        {
            const string definition = @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
";
            db.AddView(definition);
            db.DeleteView("pagesByTitle");
            var actualDefinition = db.ViewDefinitionByName("pagesByTitle");
            Assert.Null(actualDefinition);
        }

        [Fact]
        public void Cannot_get_view_instance_by_name_after_removal()
        {
            const string definition = @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
";
            db.AddView(definition);
            db.DeleteView("pagesByTitle");
            Assert.Throws<InvalidOperationException>("Cannot find a view named: 'pagesByTitle'", 
                () => db.ViewInstanceByName("pagesByTitle"));
        }

        [Fact]
        public void Removing_view_will_drop_table_for_the_values()
        {
            const string definition = @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
";
            db.AddView(definition);
            db.DeleteView("pagesByTitle");
            using (var session = new Session(db.Storage.Instance))
            {
                JET_DBID dbid;
                Api.JetOpenDatabase(session, db.Storage.Database, null, out dbid, OpenDatabaseGrbit.None);
                try
                {
                    Assert.False(Api.GetTableNames(session, dbid).Contains("views_pagesByTitle"));
                }
                finally
                {
                    Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                }
            }
        }

        [Fact]
        public void View_instances_cache_will_be_removed()
        {
            const string definition = @"var pagesByTitle = 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = (int)doc.size };
";
            db.AddView(definition);
            var actualDefinition1 = db.ViewInstanceByName("pagesByTitle");
            Assert.NotNull(actualDefinition1);
            db.DeleteView("pagesByTitle");
            Assert.Throws<InvalidOperationException>("Cannot find a view named: 'pagesByTitle'",
                () => db.ViewInstanceByName("pagesByTitle"));
     
        }

        public void Dispose()
        {
            db.Dispose();
        }
    }
}